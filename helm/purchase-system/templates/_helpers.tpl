{{/*
_helpers.tpl - Reusable template functions

DRY principle - define once, use everywhere.
This file is NOT rendered as a K8s resource (underscore prefix).
*/}}

{{/*
Create chart name and version for labels
*/}}
{{- define "purchase-system.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels applied to all resources

Labels are crucial for:
- Selecting resources (kubectl get pods -l app=web-server)
- Service selectors
- Monitoring and logging
- Cost allocation

Standard K8s recommended labels:
https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/
*/}}
{{- define "purchase-system.labels" -}}
helm.sh/chart: {{ include "purchase-system.chart" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/part-of: purchase-system
{{- end }}

{{/*
Selector labels - subset of labels used for pod selection

Don't include version in selectors - they're immutable after deployment creation.

These must match between:
- Deployment spec.selector.matchLabels
- Deployment spec.template.metadata.labels
- Service spec.selector
*/}}
{{- define "purchase-system.selectorLabels" -}}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
