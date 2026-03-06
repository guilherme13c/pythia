{{/*
Expand the name of the chart.
*/}}
{{- define "pythia.name" -}}
{{- .Chart.Name }}
{{- end }}

{{/*
Create a default fully qualified app name.
Truncate at 63 characters (Kubernetes label limit).
*/}}
{{- define "pythia.fullname" -}}
{{- if .Release.Name | eq "RELEASE-NAME" }}
{{- .Chart.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{/*
Common labels attached to every resource.
*/}}
{{- define "pythia.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels for a given component (pass component name as $.component).
*/}}
{{- define "pythia.selectorLabels" -}}
app.kubernetes.io/name: {{ include "pythia.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end }}
