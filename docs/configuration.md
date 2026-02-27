# Configuration Guide

## Environment
The environment field is used to filter logs, metrics, and other operational data. It is important to provide a
meaningful value that reflects the context in which the application is running
(e.g., `Development`, `Staging`, `Production`). This will make it easier to track and manage data for specific
environments.

## Service Account
A service account is automatically created. You can add specific annotations and specify the name of the service
account. If you want to use an existing service account, you can specify the name of the service account and disable
the service account creation.

## Custom Resource Definitions (CRDs)
CRDs for this operator are automatically created by default. You can change this by setting
`customResourceDefinitions.create` to `false`.

## Role Based Access Control Configuration
RBAC resources are created for different roles, allowing the service account to list, view, and update custom 
resources. Each role can be customized with additional labels and annotations.

## Additional Labels and Annotations
Additional labels and annotations can be added to the stream classes.

## Job Template Settings
By default the plugin Helm chart creates a job template used by operator to created a job. Additional labels and
annotations can be added to the streaming jobs and pods. You can also specify extra environment variables,
volumes, and volume mounts. You can change this by setting `customResourceDefinitions.create` to `false`
and providing your own job template in the StreamDefinition resource.

## Resource Constraints
By default, no specific resource constraints are set. If necessary, you can specify CPU and memory requests and limits.

## Security Context
The security context is set to disallow privilege escalation, drop all capabilities, enforce a read-only root 
filesystem, run as non-root, and run as user 1000. The seccomp profile is set to RuntimeDefault.

## Pod Failure Policy Settings
You can specify a list of exit codes that should trigger a retry without incrementing the retry count. By default, 
exit code 2 is set to trigger a retry. Please note that the actual exit code may vary depending on the application.
