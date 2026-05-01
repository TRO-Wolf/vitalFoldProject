from __future__ import annotations

import time
from collections.abc import Mapping
from contextlib import closing, contextmanager
from functools import cached_property
from typing import TYPE_CHECKING, Any, Literal

import psycopg as psg
from airflow.exceptions import AirflowException, AirflowNotFoundException
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from airflow.providers.common.sql.hooks.sql import DbApiHook
from polars import DataFrame as PolarsDataFrame
from sqlalchemy.engine import Engine, URL
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

if TYPE_CHECKING:
    from pandas import DataFrame as PandasDataFrame
    from mypy_boto3_dsql.client import AuroraDSQLClient
    from mypy_boto3_dsql.type_defs import (
        GetClusterOutputTypeDef,
        ListClustersOutputTypeDef,
        TagResourceOutputTypeDef,
    )

PEM_PATH = "plugins/files/global-bundle.pem"
CA_PATH = "plugins/files/AmazonRootCA1.crt"



class DSQLGenericHook(AwsGenericHook["AuroraDSQLClient"]):
    """
    Interact with AWS Aurora DSQL service.

    Amazon Aurora DSQL is a serverless, distributed SQL database suitable for
    workloads of any size. This hook provides methods to manage DSQL clusters,
    generate authentication tokens, and manage cluster policies.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
        - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dsql.html

    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param region_name: AWS region name to use for DSQL operations.
    """

    conn_name_attr = "aws_conn_id"
    default_conn_name = "aws_dsql_default"
    conn_type = "aws"
    hook_name = "Amazon Web Services"

    def __init__(
        self,
        default_cluster: str | None = None, 
        default_host: str | None = None,
        *args, 
        **kwargs
    ) -> None:
        kwargs["client_type"] = "dsql"
        super().__init__(*args, **kwargs)
        self.ca_path = CA_PATH
        self.aws_conn_id = kwargs.get("aws_conn_id", self.default_conn_name)
        self.default_cluster = self._resolve_cluster(default_cluster)
        self.default_host = self._resolve_host(default_host)

    def _resolve_cluster(self, cluster_identifier: str | None = None) -> str | None:
        """
        Resolve the cluster identifier from the argument or connection extras.

        Returns ``None`` when not available (e.g. when the hook is used
        purely for token generation).

        :param cluster_identifier: The ARN or identifier of the DSQL cluster.
        :return: Resolved cluster identifier string, or ``None``.
        """
        if cluster_identifier:
            return cluster_identifier

        conn = self.get_connection(self.aws_conn_id)
        return conn.extra_dejson.get("default_cluster")

    def _resolve_host(self, host: str | None = None) -> str | None:
        """
        Resolve the host from the argument or connection extras.

        Returns ``None`` when not available.

        :param host: The host address of the DSQL cluster.
        :return: Resolved host string, or ``None``.
        """
        if host:
            return host

        conn = self.get_connection(self.aws_conn_id)
        return conn.extra_dejson.get("default_host")

    # Cluster Management Methods

    def create_cluster(
        self,
        deletion_protection_enabled: bool = True,
        tags: dict[str, str] | None = None,
        client_token: str | None = None,
        **kwargs,
    ) -> str:
        """
        Create a new Aurora DSQL cluster.

        :param deletion_protection_enabled: Whether to enable deletion protection.
        :param tags: Dictionary of tags to apply to the cluster.
        :param client_token: Idempotency token for cluster creation.
        :param kwargs: Additional parameters passed to create_cluster API call.
        :return: The cluster ARN.
        """
        self.log.info("Creating Aurora DSQL cluster with deletion_protection=%s", deletion_protection_enabled)

        params: dict[str, Any] = {
            "deletionProtectionEnabled": deletion_protection_enabled,
        }

        if tags:
            params["tags"] = tags
        if client_token:
            params["clientToken"] = client_token

        params.update(kwargs)

        try:
            response = self.conn.create_cluster(**params)
            cluster_arn = response["arn"]
            self.log.info("Created Aurora DSQL cluster: %s", cluster_arn)
            return cluster_arn
        except Exception as e:
            self.log.error("Failed to create Aurora DSQL cluster: %s", e)
            raise AirflowException(f"Error creating DSQL cluster: {e}")

    def delete_cluster(
        self,
        cluster_identifier: str,
        client_token: str | None = None,
    ) -> None:
        """
        Delete an Aurora DSQL cluster.

        :param cluster_identifier: The ARN or identifier of the cluster to delete.
        :param client_token: Idempotency token for cluster deletion.
        """
        self.log.info("Deleting Aurora DSQL cluster: %s", cluster_identifier)

        params: dict[str, Any] = {"identifier": cluster_identifier}
        if client_token:
            params["clientToken"] = client_token

        try:
            self.conn.delete_cluster(**params)
            self.log.info("Successfully initiated deletion of cluster: %s", cluster_identifier)
        except self.conn.exceptions.ResourceNotFoundException:
            raise AirflowNotFoundException(f"Cluster not found: {cluster_identifier}")
        except Exception as e:
            self.log.error("Failed to delete cluster %s: %s", cluster_identifier, e)
            raise AirflowException(f"Error deleting DSQL cluster: {e}")
        
    def get_cluster(self, cluster_identifier: str) -> GetClusterOutputTypeDef:
        """
        Retrieve details about a specific Aurora DSQL cluster.

        :param cluster_identifier: The ARN or identifier of the cluster.
        :return: Dictionary containing cluster details.
        """
        self.log.info("Retrieving details for cluster: %s", cluster_identifier)

        try:
            response = self.conn.get_cluster(identifier=cluster_identifier)
            return response
        except self.conn.exceptions.ResourceNotFoundException:
            raise AirflowNotFoundException(f"Cluster not found: {cluster_identifier}")
        except Exception as e:
            self.log.error("Failed to retrieve cluster %s: %s", cluster_identifier, e)
            raise AirflowException(f"Error retrieving DSQL cluster: {e}")

    def get_cluster_state(self, cluster_identifier: str) -> str:
        """
        Get the current state of a DSQL cluster.

        :param cluster_identifier: The ARN or identifier of the cluster.
        :return: The cluster state (e.g., 'CREATING', 'ACTIVE', 'DELETING', 'DELETED').
        """
        cluster_info = self.get_cluster(cluster_identifier)
        state = cluster_info["status"]
        self.log.info("Cluster %s is in state: %s", cluster_identifier, state)
        return state

    def list_clusters(
        self,
        max_results: int | None = None,
        next_token: str | None = None,
    ) -> ListClustersOutputTypeDef:
        """
        List all Aurora DSQL clusters.

        :param max_results: Maximum number of results to return.
        :param next_token: Token for pagination.
        :return: Dictionary containing list of clusters and pagination info.
        """
        self.log.info("Listing Aurora DSQL clusters")

        params: dict[str, Any] = {}
        if max_results:
            params["maxResults"] = max_results
        if next_token:
            params["nextToken"] = next_token

        try:
            response = self.conn.list_clusters(**params)
            self.log.info("Found %d clusters", len(response.get("clusters", [])))
            return response
        except Exception as e:
            self.log.error("Failed to list clusters: %s", e)
            raise AirflowException(f"Error listing DSQL clusters: {e}")

    def update_cluster(
        self,
        cluster_identifier: str,
        deletion_protection_enabled: bool | None = None,
        client_token: str | None = None,
        **kwargs,
    ) -> str:
        """
        Update an existing Aurora DSQL cluster.

        :param cluster_identifier: The ARN or identifier of the cluster.
        :param deletion_protection_enabled: Whether to enable deletion protection.
        :param client_token: Idempotency token for cluster update.
        :param kwargs: Additional parameters passed to update_cluster API call.
        :return: The cluster ARN.
        """
        self.log.info("Updating Aurora DSQL cluster: %s", cluster_identifier)

        params: dict[str, Any] = {"identifier": cluster_identifier}

        if deletion_protection_enabled is not None:
            params["deletionProtectionEnabled"] = deletion_protection_enabled
        if client_token:
            params["clientToken"] = client_token

        params.update(kwargs)

        try:
            response = self.conn.update_cluster(**params)
            cluster_arn = response["arn"]
            self.log.info("Updated Aurora DSQL cluster: %s", cluster_arn)
            return cluster_arn
        except self.conn.exceptions.ResourceNotFoundException:
            raise AirflowNotFoundException(f"Cluster not found: {cluster_identifier}")
        except Exception as e:
            self.log.error("Failed to update cluster %s: %s", cluster_identifier, e)
            raise AirflowException(f"Error updating DSQL cluster: {e}")

    # Authentication Token Methods

    def generate_db_connect_auth_token(
        self,
        cluster_arn: str,
        region: str | None = None,
        expires_in: int = 900,
    ) -> str:
        """
        Generate a standard database connection authentication token.

        :param cluster_arn: The ARN of the DSQL cluster.
        :param region: AWS region (uses hook's region if not specified).
        :param expires_in: Token expiration time in seconds (default: 900 = 15 minutes).
        :return: Authentication token string.
        """
        region = region or self.region_name
        self.log.info("Generating DB connection auth token for cluster: %s", cluster_arn)

        try:
            token = self.conn.generate_db_connect_auth_token(
                clusterArn=cluster_arn,
                region=region,
                expiresIn=expires_in,
            )
            self.log.info("Successfully generated auth token (expires in %d seconds)", expires_in)
            return token
        except Exception as e:
            self.log.error("Failed to generate auth token: %s", e)
            raise AirflowException(f"Error generating DSQL auth token: {e}")

    def generate_db_connect_admin_auth_token(
        self,
        hostname: str,
        region: str | None = None,
        expires_in: int = 900,
    ) -> str:
        """
        Generate an administrative database connection authentication token.

        :param hostname: The host address of the DSQL cluster.
        :param region: AWS region (uses hook's region if not specified).
        :param expires_in: Token expiration time in seconds (default: 900 = 15 minutes).
        :return: Administrative authentication token string.
        """
        region = region or self.region_name
        self.log.info("Generating DB admin auth token for cluster: %s", hostname)

        try:
            token = self.conn.generate_db_connect_admin_auth_token(
                Hostname=hostname,
                Region=region,
                ExpiresIn=expires_in,
            )
            self.log.info("Successfully generated admin auth token (expires in %d seconds)", expires_in)
            return token
        except Exception as e:
            self.log.error("Failed to generate admin auth token: %s", e)
            raise AirflowException(f"Error generating DSQL admin auth token: {e}")

    # Cluster Policy Methods

    def get_cluster_policy(self, cluster_identifier: str) -> dict[str, Any]:
        """
        Retrieve the access control policy for a cluster.

        :param cluster_identifier: The ARN or identifier of the cluster.
        :return: Dictionary containing the cluster policy.
        """
        self.log.info("Retrieving policy for cluster: %s", cluster_identifier)

        try:
            response = self.conn.get_cluster_policy(resourceArn=cluster_identifier)
            return response
        except self.conn.exceptions.ResourceNotFoundException:
            raise AirflowNotFoundException(f"Cluster not found: {cluster_identifier}")
        except Exception as e:
            self.log.error("Failed to retrieve cluster policy: %s", e)
            raise AirflowException(f"Error retrieving DSQL cluster policy: {e}")

    def put_cluster_policy(
        self,
        cluster_identifier: str,
        policy: str,
    ) -> dict[str, Any]:
        """
        Apply an access control policy to a cluster.

        :param cluster_identifier: The ARN or identifier of the cluster.
        :param policy: The policy document in JSON string format.
        :return: Dictionary containing the result of the operation.
        """
        self.log.info("Applying policy to cluster: %s", cluster_identifier)

        try:
            response = self.conn.put_cluster_policy(
                resourceArn=cluster_identifier,
                policy=policy,
            )
            self.log.info("Successfully applied policy to cluster: %s", cluster_identifier)
            return response
        except self.conn.exceptions.ResourceNotFoundException:
            raise AirflowNotFoundException(f"Cluster not found: {cluster_identifier}")
        except Exception as e:
            self.log.error("Failed to apply cluster policy: %s", e)
            raise AirflowException(f"Error applying DSQL cluster policy: {e}")

    def delete_cluster_policy(self, cluster_identifier: str) -> dict[str, Any]:
        """
        Remove the access control policy from a cluster.

        :param cluster_identifier: The ARN or identifier of the cluster.
        :return: Dictionary containing the result of the operation.
        """
        self.log.info("Deleting policy from cluster: %s", cluster_identifier)

        try:
            response = self.conn.delete_cluster_policy(resourceArn=cluster_identifier)
            self.log.info("Successfully deleted policy from cluster: %s", cluster_identifier)
            return response
        except self.conn.exceptions.ResourceNotFoundException:
            raise AirflowNotFoundException(f"Cluster not found: {cluster_identifier}")
        except Exception as e:
            self.log.error("Failed to delete cluster policy: %s", e)
            raise AirflowException(f"Error deleting DSQL cluster policy: {e}")

    # Tag Management Methods

    def tag_resource(
        self,
        resource_arn: str,
        tags: dict[str, str],
    ) -> TagResourceOutputTypeDef:
        """
        Add or update tags on a DSQL resource.

        :param resource_arn: The ARN of the resource to tag.
        :param tags: Dictionary of tags to apply.
        :return: Dictionary containing the result of the operation.
        """
        self.log.info("Adding tags to resource: %s", resource_arn)

        try:
            response = self.conn.tag_resource(
                resourceArn=resource_arn,
                tags=tags,
            )
            self.log.info("Successfully tagged resource with %d tags", len(tags))
            return response
        except self.conn.exceptions.ResourceNotFoundException:
            raise AirflowNotFoundException(f"Resource not found: {resource_arn}")
        except Exception as e:
            self.log.error("Failed to tag resource: %s", e)
            raise AirflowException(f"Error tagging DSQL resource: {e}")

    def untag_resource(
        self,
        resource_arn: str,
        tag_keys: list[str],
    ) -> dict[str, Any]:
        """
        Remove tags from a DSQL resource.

        :param resource_arn: The ARN of the resource to untag.
        :param tag_keys: List of tag keys to remove.
        :return: Dictionary containing the result of the operation.
        """
        self.log.info("Removing tags from resource: %s", resource_arn)

        try:
            response = self.conn.untag_resource(
                resourceArn=resource_arn,
                tagKeys=tag_keys,
            )
            self.log.info("Successfully removed %d tags from resource", len(tag_keys))
            return response
        except self.conn.exceptions.ResourceNotFoundException:
            raise AirflowNotFoundException(f"Resource not found: {resource_arn}")
        except Exception as e:
            self.log.error("Failed to untag resource: %s", e)
            raise AirflowException(f"Error untagging DSQL resource: {e}")

    def list_tags_for_resource(self, resource_arn: str) -> dict[str, str]:
        """
        List all tags on a DSQL resource.

        :param resource_arn: The ARN of the resource.
        :return: Dictionary of tags.
        """
        self.log.info("Listing tags for resource: %s", resource_arn)

        try:
            response = self.conn.list_tags_for_resource(resourceArn=resource_arn)
            tags = response.get("tags", {})
            self.log.info("Found %d tags on resource", len(tags))
            return tags
        except self.conn.exceptions.ResourceNotFoundException:
            raise AirflowNotFoundException(f"Resource not found: {resource_arn}")
        except Exception as e:
            self.log.error("Failed to list tags: %s", e)
            raise AirflowException(f"Error listing DSQL resource tags: {e}")

    # VPC Endpoint Methods

    def get_vpc_endpoint_service_name(self, cluster_identifier: str) -> str:
        """
        Get the VPC endpoint service name for a cluster.

        :param cluster_identifier: The ARN or identifier of the cluster.
        :return: The VPC endpoint service name.
        """
        self.log.info("Retrieving VPC endpoint service name for cluster: %s", cluster_identifier)

        try:
            response = self.conn.get_vpc_endpoint_service_name(clusterArn=cluster_identifier)
            service_name = response.get("vpcEndpointServiceName", "")
            self.log.info("VPC endpoint service name: %s", service_name)
            return service_name
        except self.conn.exceptions.ResourceNotFoundException:
            raise AirflowNotFoundException(f"Cluster not found: {cluster_identifier}")
        except Exception as e:
            self.log.error("Failed to get VPC endpoint service name: %s", e)
            raise AirflowException(f"Error getting VPC endpoint service name: {e}")

    # Waiter Methods

    def wait_for_cluster_state(
        self,
        cluster_identifier: str,
        target_state: str,
        check_interval: int = 30,
        max_attempts: int = 40,
    ) -> None:
        """
        Wait for a DSQL cluster to reach a specific state.

        :param cluster_identifier: The ARN or identifier of the cluster.
        :param target_state: The desired state (e.g., 'ACTIVE', 'DELETED').
        :param check_interval: Time in seconds between state checks.
        :param max_attempts: Maximum number of attempts before timing out.
        """
        self.log.info(
            "Waiting for cluster %s to reach state '%s' (max attempts: %d, interval: %ds)",
            cluster_identifier,
            target_state,
            max_attempts,
            check_interval,
        )

        if target_state.upper() == "ACTIVE":
            try:
                waiter = self.conn.get_waiter("cluster_active")
                waiter.wait(
                    identifier=cluster_identifier,
                    WaiterConfig={
                        "Delay": check_interval,
                        "MaxAttempts": max_attempts,
                    },
                )
                self.log.info("Cluster %s is now ACTIVE", cluster_identifier)
                return
            except Exception as e:
                self.log.error("Error waiting for cluster to become active: %s", e)
                raise AirflowException(f"Cluster did not reach ACTIVE state: {e}")

        elif target_state.upper() in ("DELETED", "NOT_EXISTS"):
            try:
                waiter = self.conn.get_waiter("cluster_not_exists")
                waiter.wait(
                    identifier=cluster_identifier,
                    WaiterConfig={
                        "Delay": check_interval,
                        "MaxAttempts": max_attempts,
                    },
                )
                self.log.info("Cluster %s is now DELETED", cluster_identifier)
                return
            except Exception as e:
                self.log.error("Error waiting for cluster deletion: %s", e)
                raise AirflowException(f"Cluster was not deleted: {e}")

        else:
            # Custom polling for other states
            self._wait_for_state(
                cluster_identifier=cluster_identifier,
                target_state=target_state,
                check_interval=check_interval,
                max_attempts=max_attempts,
            )

    def _wait_for_state(
        self,
        cluster_identifier: str,
        target_state: str,
        check_interval: int,
        max_attempts: int,
    ) -> None:
        """
        Custom polling implementation for cluster state transitions.

        :param cluster_identifier: The ARN or identifier of the cluster.
        :param target_state: The desired state.
        :param check_interval: Time in seconds between state checks.
        :param max_attempts: Maximum number of attempts before timing out.
        """
        for attempt in range(1, max_attempts + 1):
            try:
                current_state = self.get_cluster_state(cluster_identifier)

                if current_state.upper() == target_state.upper():
                    self.log.info(
                        "Cluster %s reached target state '%s' after %d attempts",
                        cluster_identifier,
                        target_state,
                        attempt,
                    )
                    return

                self.log.info(
                    "Cluster %s is in state '%s', waiting for '%s' (attempt %d/%d)",
                    cluster_identifier,
                    current_state,
                    target_state,
                    attempt,
                    max_attempts,
                )

                if attempt < max_attempts:
                    time.sleep(check_interval)

            except AirflowNotFoundException:
                if target_state.upper() in ("DELETED", "NOT_EXISTS"):
                    self.log.info("Cluster %s no longer exists (deleted)", cluster_identifier)
                    return
                raise

        raise AirflowException(
            f"Cluster {cluster_identifier} did not reach state '{target_state}' "
            f"after {max_attempts} attempts"
        )

    # Utility Methods

    def test_connection(self) -> tuple[bool, str]:
        """
        Test the AWS connection by listing clusters.

        :return: Tuple of (success: bool, message: str)
        """
        try:
            response = self.conn.list_clusters(maxResults=1)
            cluster_count = len(response.get("clusters", []))
            return (
                True,
                f"Successfully connected to Aurora DSQL. Found {cluster_count} cluster(s).",
            )
        except Exception as e:
            return False, f"Connection test failed: {str(e)}"

    def get_paginator(self, operation_name: str):
        """
        Get a paginator for the specified operation.

        :param operation_name: The operation name (e.g., 'list_clusters').
        :return: A paginator object.
        """
        return self.conn.get_paginator(operation_name)
    



class DSQLSqlHook(DbApiHook):
    """
    Interact with AWS Aurora DSQL using SQLAlchemy and DB-API.

    Follows the canonical Airflow pattern (like ``RedshiftSQLHook``): inherits
    from ``DbApiHook`` for standard SQL methods and uses ``DSQLGenericHook``
    via **composition** for AWS IAM token generation.

    :param dsql_conn_id: The Airflow connection used for DSQL host/extras.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        For backward compatibility, if only ``aws_conn_id`` is provided it is
        used as both the SQL and AWS connection ID.
    :param cluster_identifier: The ARN or identifier of the DSQL cluster.
    :param default_host: The host address of the DSQL cluster.
    :param database: Database name to connect to (default: ``"postgres"``).
    :param use_admin_token: Whether to use admin authentication token.
    :param token_expires_in: Token expiration time in seconds.
    """

    conn_name_attr = "dsql_conn_id"
    default_conn_name = "aws_dsql_default"
    conn_type = "dsql"
    hook_name = "Aurora DSQL"
    supports_autocommit = True

    def __init__(
        self,
        dsql_conn_id: str = "aws_dsql_default",
        aws_conn_id: str | None = None,
        cluster_identifier: str | None = None,
        default_host: str | None = None,
        database: str = "postgres",
        use_admin_token: bool = True,
        token_expires_in: int = 600_000,
        **kwargs,
    ):
        # Backward compat: old callers pass aws_conn_id as the only ID
        if aws_conn_id and dsql_conn_id == "aws_dsql_default":
            dsql_conn_id = aws_conn_id
        kwargs[self.conn_name_attr] = dsql_conn_id
        super().__init__(**kwargs)

        self.aws_conn_id = aws_conn_id or dsql_conn_id
        self._cluster_identifier = cluster_identifier
        self._default_host = default_host
        self.database = database
        self.use_admin_token = use_admin_token
        self.token_expires_in = token_expires_in
        self.ca_path = CA_PATH
        self._engine: Engine | None = None
        self._async_engine: AsyncEngine | None = None

    # ── AWS composition ──────────────────────────────────────────────────

    @cached_property
    def _aws_hook(self) -> DSQLGenericHook:
        """Lazily create a DSQLGenericHook for token generation."""
        return DSQLGenericHook(
            default_cluster=self._cluster_identifier,
            default_host=self._default_host,
            aws_conn_id=self.aws_conn_id,
        )

    # ── Lazy host / cluster resolution ───────────────────────────────────

    @cached_property
    def host(self) -> str:
        """Resolve DSQL host from init arg, cluster_identifier, or connection extras."""
        if self._default_host:
            return self._default_host
        # Fall back to cluster_identifier so callers only need to specify the
        # host once (as either default_host or cluster_identifier).
        if self._cluster_identifier:
            return self._cluster_identifier
        conn = self.get_connection(self.get_conn_id())
        host = conn.extra_dejson.get("default_host") or conn.host
        if not host:
            raise AirflowException(
                "default_host not set in __init__ or connection extras."
            )
        return host

    @cached_property
    def cluster_identifier(self) -> str:
        """Resolve DSQL cluster identifier from init arg or connection extras."""
        if self._cluster_identifier:
            return self._cluster_identifier
        conn = self.get_connection(self.get_conn_id())
        cluster = conn.extra_dejson.get("default_cluster")
        if not cluster:
            raise AirflowException(
                "cluster_identifier not set in __init__ or connection extras."
            )
        return cluster

    # ── Token generation (single source of truth) ────────────────────────

    def _generate_token(self) -> str:
        """Generate a DSQL IAM auth token via the composed AWS hook."""
        if self.use_admin_token:
            return self._aws_hook.generate_db_connect_admin_auth_token(
                hostname=self.host,
                region=self._aws_hook.region_name,
                expires_in=self.token_expires_in,
            )
        return self._aws_hook.generate_db_connect_auth_token(
            cluster_arn=self.host,
            region=self._aws_hook.region_name,
            expires_in=self.token_expires_in,
        )

    # ── DbApiHook contract ───────────────────────────────────────────────

    @property
    def sqlalchemy_url(self) -> URL:
        """Return a PostgreSQL-compatible SQLAlchemy URL with IAM token."""
        return URL.create(
            drivername="postgresql+psycopg2",
            username="admin",
            password=self._generate_token(),
            host=self.host,
            port=5432,
            database=self.database,
            query={
                "sslmode": "verify-full",
                "sslrootcert": self.ca_path,
            },
        )

    def get_conn(self):
        """
        Return a raw psycopg DB-API connection to the DSQL cluster.

        :return: psycopg connection
        """
        return psg.connect(**self._get_conn_params())

    def _get_conn_params(self) -> dict[str, Any]:
        """Build psycopg connection keyword arguments."""
        return {
            "dbname": self.database,
            "user": "admin",
            "host": self.host,
            "password": self._generate_token(),
            "port": 5432,
            "sslmode": "verify-full",
            "sslrootcert": self.ca_path,
        }

    # ── Convenience aliases expected by DAGs ─────────────────────────────

    def get_engine(self) -> Engine:
        """
        Return a SQLAlchemy engine (delegates to inherited
        ``get_sqlalchemy_engine``).

        :return: SQLAlchemy Engine instance
        """
        return self.get_sqlalchemy_engine()

    @contextmanager
    def cursor(self, engine_kwargs: dict[str, Any] | None = None):
        """
        Context manager that provides a psycopg cursor with autocommit.

        :param engine_kwargs: Unused, kept for backward compatibility.
        :yield: psycopg cursor
        """
        conn = psg.connect(**self._get_conn_params(), autocommit=True)
        cur = conn.cursor()
        try:
            yield cur
        finally:
            cur.close()
            conn.close()

    # ── DataFrame helpers ────────────────────────────────────────────────

    def _get_pandas_df(
        self,
        sql: str | list[str],
        parameters: list | tuple | Mapping[str, Any] | None = None,
        **kwargs,
    ) -> PandasDataFrame:
        """
        Execute SQL and return a pandas DataFrame.

        :param sql: SQL statement(s) to execute.
        :param parameters: Query parameters.
        :param kwargs: Passed into ``pandas.io.sql.read_sql``.
        """
        try:
            from pandas.io import sql as psql
        except ImportError as e:
            raise AirflowException(
                "pandas library not installed, run: pip install pandas"
            ) from e

        with closing(self.get_conn()) as conn:
            return psql.read_sql(sql, con=conn, params=parameters, **kwargs)

    def _get_polars_df(
        self,
        sql: str | list[str],
        parameters: list | tuple | Mapping[str, Any] | None = None,
        **kwargs,
    ) -> PolarsDataFrame:
        """
        Execute SQL and return a polars DataFrame.

        :param sql: SQL statement(s) to execute.
        :param parameters: Query parameters.
        :param kwargs: Passed into ``polars.read_database``.
        """
        try:
            import polars as pl
        except ImportError as e:
            raise AirflowException(
                "polars library not installed, run: pip install polars"
            ) from e

        with closing(self.get_conn()) as conn:
            execute_options: dict[str, Any] | None = None
            if parameters is not None:
                if isinstance(parameters, Mapping):
                    execute_options = dict(parameters)
                else:
                    execute_options = {}

            return pl.read_database(
                sql,
                connection=conn,
                execute_options=execute_options,
                infer_schema_length=5_000,  # Disable schema inference for better performance
                **kwargs,
            )

    def get_df(
        self,
        sql: str | list[str],
        parameters: list | tuple | Mapping[str, Any] | None = None,
        *,
        df_type: Literal["pandas", "polars"] = "pandas",
        **kwargs,
    ) -> PandasDataFrame | PolarsDataFrame:
        """
        Execute SQL and return a DataFrame.

        :param sql: SQL statement(s) to execute.
        :param parameters: Query parameters.
        :param df_type: ``"pandas"`` or ``"polars"``.
        :param kwargs: Passed to the underlying read function.
        """
        if df_type == "pandas":
            return self._get_pandas_df(sql, parameters, **kwargs)
        if df_type == "polars":
            return self._get_polars_df(sql, parameters, **kwargs)
        raise ValueError(f"Unsupported df_type: {df_type}")

    # ── Async support ────────────────────────────────────────────────────

    @property
    def _async_sqlalchemy_url(self) -> URL:
        """Return an asyncpg-compatible SQLAlchemy URL with IAM token."""
        return URL.create(
            drivername="postgresql+asyncpg",
            username="admin",
            password=self._generate_token(),
            host=self.host,
            port=5432,
            database=self.database,
            query={
                "sslmode": "verify-full",
                "sslrootcert": self.ca_path,
            },
        )

    def get_async_sqlalchemy_engine(
        self,
        engine_kwargs: dict[str, Any] | None = None,
    ) -> AsyncEngine:
        """
        Get or create an async SQLAlchemy engine for the DSQL cluster.

        :param engine_kwargs: Additional arguments for ``create_async_engine``.
        :return: SQLAlchemy AsyncEngine instance.
        """
        if self._async_engine is not None:
            return self._async_engine

        self.log.info(
            "Creating async SQLAlchemy engine for cluster: %s",
            self.cluster_identifier,
        )
        self._async_engine = create_async_engine(
            self._async_sqlalchemy_url, **(engine_kwargs or {})
        )
        return self._async_engine

    # ── Cleanup ──────────────────────────────────────────────────────────

    def dispose_engine(self) -> None:
        """Dispose of SQLAlchemy engines and clean up resources."""
        if self._engine:
            self._engine.dispose()
            self._engine = None
        if self._async_engine:
            self._async_engine = None



