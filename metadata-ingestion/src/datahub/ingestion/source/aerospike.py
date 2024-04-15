import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Iterable, List, Optional, Tuple, Type, Union, ValuesView

# import bson.timestamp
# import pymongo.collection
from packaging import version
from pydantic import PositiveInt, validator
from pydantic.fields import Field
# from pymongo.aerospike_client import MongoClient

import aerospike

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import MetadataWorkUnitProcessor
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.schema_inference.object import (
    SchemaDescription,
    construct_schema,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
    StatefulIngestionConfigBase,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaField,
    SchemaFieldDataType,
    SchemalessClass,
    SchemaMetadata,
    StringTypeClass,
    TimeTypeClass,
    UnionTypeClass,
)
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
)

logger = logging.getLogger(__name__)

DENY_NAMESPACE_LIST = set([])

class AuthMode(Enum):
    AUTH_EXTERNAL = aerospike.AUTH_INTERNAL
    AUTH_EXTERNAL_INSECURE = aerospike.AUTH_EXTERNAL_INSECURE
    AUTH_INTERNAL = aerospike.AUTH_INTERNAL


class AerospikeConfig(
    PlatformInstanceConfigMixin, EnvConfigMixin, StatefulIngestionConfigBase
):
    # See the MongoDB authentication docs for details and examples.
    # https://pymongo.readthedocs.io/en/stable/examples/authentication.html
    hosts: list[tuple] = Field(
        default=[("localhost", "3000")], description="Aerospike hosts list."
    )
    user: Optional[str] = Field(default=None, description="Aerospike username.")
    password: Optional[str] = Field(default=None, description="Aerospike password.")
    auth_mode: Optional[AuthMode] = Field(
        default=AuthMode.AUTH_INTERNAL, description="The authentication mode with the server."
    )
    tls_enabled: bool = Field(
        default=False, description="Whether to use TLS for the connection."
    )
    tls_ca_cert_path: Optional[str] = Field(
        default=None, description="Path to the CA certificate file."
    )
    tls_cafile: Optional[str] = Field(
        default=None, description="Path to the CA certificate file."
    )
    enableSchemaInference: bool = Field(
        default=True, description="Whether to infer schemas. "
    )
    schemaSamplingSize: Optional[PositiveInt] = Field(
        default=1000,
        description="Number of documents to use when inferring schema. If set to `null`, all documents will be scanned.",
    )
    maxSchemaSize: Optional[PositiveInt] = Field(
        default=1024, description="Maximum number of fields to include in the schema."
    )
    namespace_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="regex patterns for namespaces to filter in ingestion.",
    )
    set_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="regex patterns for sets to filter in ingestion.",
    )
    ignore_empty_sets: bool = Field(
        default=False, description="Ignore empty sets in the schema inference."
    )
    # Custom Stateful Ingestion settings
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None


@dataclass
class AerospikeSourceReport(StaleEntityRemovalSourceReport):
    filtered: List[str] = field(default_factory=list)

    def report_dropped(self, name: str) -> None:
        self.filtered.append(name)


# map PyMongo types to canonical MongoDB strings
# PYMONGO_TYPE_TO_MONGO_TYPE = {
#     list: "ARRAY",
#     dict: "OBJECT",
#     type(None): "null",
#     bool: "boolean",
#     int: "integer",
#     bson.int64.Int64: "biginteger",
#     float: "float",
#     str: "string",
#     bson.datetime.datetime: "date",
#     bson.timestamp.Timestamp: "timestamp",
#     bson.dbref.DBRef: "dbref",
#     bson.objectid.ObjectId: "oid",
#     bson.Decimal128: "numberDecimal",
#     "mixed": "mixed",
# }

# # map PyMongo types to DataHub classes
# _field_type_mapping: Dict[Union[Type, str], Type] = {
#     list: ArrayTypeClass,
#     bool: BooleanTypeClass,
#     type(None): NullTypeClass,
#     int: NumberTypeClass,
#     bson.int64.Int64: NumberTypeClass,
#     float: NumberTypeClass,
#     str: StringTypeClass,
#     bson.datetime.datetime: TimeTypeClass,
#     bson.timestamp.Timestamp: TimeTypeClass,
#     bson.dbref.DBRef: BytesTypeClass,
#     bson.objectid.ObjectId: BytesTypeClass,
#     bson.Decimal128: NumberTypeClass,
#     dict: RecordTypeClass,
#     "mixed": UnionTypeClass,
# }


class AerospikeSet:
    def __init__(self, info_string: str):
        self.ns: str = None
        self.set: str = None
        self.objects: int = None
        self.tombstones: int = None
        self.memory_data_bytes: int = None
        self.device_data_bytes: int = None
        self.truncate_lut: int = None
        self.sindexes: int = None
        self.index_populating: bool = None
        self.disable_eviction: bool = None
        self.enable_index: bool = None
        self.stop_writes_count: int = None
        self.truncating: bool = None
        self.stop_writes_size: int = None
        self.set_enable_xdr: str = None

        info_list = info_string.split(':')
        for item in info_list:
            key, value = item.split('=')
            setattr(self, key, value)


def construct_schema_aeropsike(
    set: AerospikeSet,
    delimiter: str,
    max_document_size: int,
    should_add_document_size_filter: bool,
    sample_size: Optional[int] = None,
) -> Dict[Tuple[str, ...], SchemaDescription]:
    """
    Calls construct_schema on a PyMongo collection.

    Returned schema is keyed by tuples of nested field names, with each
    value containing 'types', 'count', 'nullable', 'delimited_name', and 'type' attributes.

    Parameters
    ----------
        set:
            the Aerospike collection
        delimiter:
            string to concatenate field names by
        max_document_size:
            maximum size of the document that will be considered for generating the schema.
        should_add_document_size_filter:
            boolean to indicate if document size filter should be added to aggregation
        sample_size:
            number of items in the collection to sample
            (reads entire collection if not provided)
    """

    aggregations: List[Dict] = []
    if should_add_document_size_filter:
        doc_size_field = "temporary_doc_size_field"
        # create a temporary field to store the size of the document. filter on it and then remove it.
        aggregations = [
            {"$addFields": {doc_size_field: {"$bsonSize": "$$ROOT"}}},
            {"$match": {doc_size_field: {"$lt": max_document_size}}},
            {"$project": {doc_size_field: 0}},
        ]
    if use_random_sampling:
        # get sample documents in collection
        if sample_size:
            aggregations.append({"$sample": {"size": sample_size}})
        documents = collection.aggregate(
            aggregations,
            allowDiskUse=True,
        )
    else:
        if sample_size:
            aggregations.append({"$limit": sample_size})
        documents = collection.aggregate(aggregations, allowDiskUse=True)

    return construct_schema(list(documents), delimiter)


@platform_name("MongoDB")
@config_class(AerospikeConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@dataclass
class AerospikeSource(StatefulIngestionSourceBase):
    """
    This plugin extracts the following:

    - Namespaces and associated metadata
    - Sets in each namespace and schemas for each set (via schema inference)

    By default, schema inference samples 1,000 documents from each collection. Setting `schemaSamplingSize: null` will scan the entire collection.
    Moreover, setting `useRandomSampling: False` will sample the first documents found without random selection, which may be faster for large collections.

    Note that `schemaSamplingSize` has no effect if `enableSchemaInference: False` is set.

    Really large schemas will be further truncated to a maximum of 300 schema fields. This is configurable using the `maxSchemaSize` parameter.

    """

    config: AerospikeConfig
    report: AerospikeSourceReport
    aerospike_client: aerospike.client

    def __init__(self, ctx: PipelineContext, config: AerospikeConfig):
        super().__init__(config, ctx)
        self.config = config
        self.report = AerospikeSourceReport()

        client_config = {"hosts": self.config.hosts}
        if self.config.user is not None:
            client_config["user"] = self.config.user
        if self.config.password is not None:
            client_config["password"] = self.config.password
        if self.config.auth_mode is not None:
            client_config["auth_mode"] = self.config.auth_mode
        if self.config.tls_enabled is not None:
            client_config["tls"] = {}
            client_config["tls"]["tls_enabled"] = self.config.tls_enabled
        if self.config.tls_ca_cert_path is not None and self.config.tls_enabled:
            client_config["tls"]["tls_ca_cert_path"] = self.config.tls_ca_cert_path
        if self.config.tls_cafile is not None and self.config.tls_enabled:
            client_config["tls"]["tls_cafile"] = self.config.tls_cafile

        # See https://pymongo.readthedocs.io/en/stable/examples/datetimes.html#handling-out-of-range-datetimes
        self.aerospike_client = aerospike.client(client_config).connect()  # type: ignore

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "AerospikeSource":
        config = AerospikeConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_field_type(
        self, field_type: Union[Type, str], set_name: str
    ) -> SchemaFieldDataType:
        """
        Maps types encountered in PyMongo to corresponding schema types.

        Parameters
        ----------
            field_type:
                type of a Python object
            set_name:
                name of collection (for logging)
        """
        TypeClass: Optional[Type] = _field_type_mapping.get(field_type)

        if TypeClass is None:
            self.report.report_warning(
                set_name, f"unable to map type {field_type} to metadata schema"
            )
            TypeClass = NullTypeClass

        return SchemaFieldDataType(type=TypeClass())

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        platform = "aerospike"

        sets_info: str = self.aerospike_client.info_random_node(f"sets").removeprefix('sets\t').removesuffix(';\n')
        all_sets: List[AerospikeSet] = [AerospikeSet(item) for item in sets_info.split(';') if item]
        if self.config.ignore_empty_sets:
            all_sets = [aerospike_set for aerospike_set in all_sets if aerospike_set.objects > 0]

        namespaces = list(set([aerospike_set.ns for aerospike_set in all_sets]))

        # traverse namespaces in sorted order so output is consistent
        for namespace in sorted(namespaces):
            if namespace in DENY_NAMESPACE_LIST:
                continue
            if not self.config.namespace_pattern.allowed(namespace):
                self.report.report_dropped(namespace)
                continue

            # database = self.aerospike_client[namespace]
            ns_sets: List[str] = [aerospike_set.set for aerospike_set in all_sets if aerospike_set.ns == namespace]

            # traverse collections in sorted order so output is consistent
            for curr_set in sorted(ns_sets):
                dataset_name = f"{namespace}.{curr_set}"

                if not self.config.set_pattern.allowed(dataset_name):
                    self.report.report_dropped(dataset_name)
                    continue

                dataset_urn = make_dataset_urn_with_platform_instance(
                    platform=platform,
                    name=dataset_name,
                    env=self.config.env,
                    platform_instance=self.config.platform_instance,
                )

                # Initialize data_platform_instance with a default value
                data_platform_instance = None
                if self.config.platform_instance:
                    data_platform_instance = DataPlatformInstanceClass(
                        platform=make_data_platform_urn(platform),
                        instance=make_dataplatform_instance_urn(
                            platform, self.config.platform_instance
                        ),
                    )

                dataset_properties = DatasetPropertiesClass(
                    tags=[],
                    customProperties={},
                )

                if self.config.enableSchemaInference:
                    collection_schema = construct_schema_aeropsike(
                        curr_set.set,
                        delimiter=".",
                        should_add_document_size_filter=self.should_add_document_size_filter(),
                        sample_size=self.config.schemaSamplingSize,
                    )

                    # initialize the schema for the collection
                    canonical_schema: List[SchemaField] = []
                    max_schema_size = self.config.maxSchemaSize
                    collection_schema_size = len(collection_schema.values())
                    collection_fields: Union[
                        List[SchemaDescription], ValuesView[SchemaDescription]
                    ] = collection_schema.values()
                    assert max_schema_size is not None
                    if collection_schema_size > max_schema_size:
                        # downsample the schema, using frequency as the sort key
                        self.report.report_warning(
                            key=dataset_urn,
                            reason=f"Downsampling the collection schema because it has {collection_schema_size} fields. Threshold is {max_schema_size}",
                        )
                        # Add this information to the custom properties so user can know they are looking at downsampled schema
                        dataset_properties.customProperties[
                            "schema.downsampled"
                        ] = "True"
                        dataset_properties.customProperties[
                            "schema.totalFields"
                        ] = f"{collection_schema_size}"

                    logger.debug(
                        f"Size of collection fields = {len(collection_fields)}"
                    )
                    # append each schema field (sort so output is consistent)
                    for schema_field in sorted(
                        collection_fields,
                        key=lambda x: (
                            -x["count"],
                            x["delimited_name"],
                        ),  # Negate `count` for descending order, `delimited_name` stays the same for ascending
                    )[0:max_schema_size]:
                        field = SchemaField(
                            fieldPath=schema_field["delimited_name"],
                            nativeDataType=self.get_pymongo_type_string(
                                schema_field["type"], dataset_name
                            ),
                            type=self.get_field_type(
                                schema_field["type"], dataset_name
                            ),
                            description=None,
                            nullable=schema_field["nullable"],
                            recursive=False,
                        )
                        canonical_schema.append(field)

                    # create schema metadata object for collection
                    schema_metadata = SchemaMetadata(
                        schemaName=set_name,
                        platform=f"urn:li:dataPlatform:{platform}",
                        version=0,
                        hash="",
                        platformSchema=SchemalessClass(),
                        fields=canonical_schema,
                    )

                # TODO: use list_indexes() or index_information() to get index information
                # See https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html#pymongo.collection.Collection.list_indexes.

                yield from [
                    mcp.as_workunit()
                    for mcp in MetadataChangeProposalWrapper.construct_many(
                        entityUrn=dataset_urn,
                        aspects=[
                            schema_metadata,
                            dataset_properties,
                            data_platform_instance,
                        ],
                    )
                ]

    def is_server_version_gte_4_4(self) -> bool:
        try:
            server_version = self.aerospike_client.server_info().get("versionArray")
            if server_version:
                logger.info(
                    f"Mongodb version for current connection - {server_version}"
                )
                server_version_str_list = [str(i) for i in server_version]
                required_version = "4.4"
                return version.parse(
                    ".".join(server_version_str_list)
                ) >= version.parse(required_version)
        except Exception as e:
            logger.error("Error while getting version of the mongodb server %s", e)

        return False

    def is_hosted_on_aws_documentdb(self) -> bool:
        return self.config.hostingEnvironment == HostingEnvironment.AWS_DOCUMENTDB

    def should_add_document_size_filter(self) -> bool:
        # the operation $bsonsize is only available in server version greater than 4.4
        # and is not supported by AWS DocumentDB, we should only add this operation to
        # aggregation for mongodb that doesn't run on AWS DocumentDB and version is greater than 4.4
        # https://docs.aws.amazon.com/documentdb/latest/developerguide/mongo-apis.html
        return (
            self.is_server_version_gte_4_4() and not self.is_hosted_on_aws_documentdb()
        )

    def get_report(self) -> AerospikeSourceReport:
        return self.report

    def close(self):
        self.aerospike_client.close()
        super().close()
