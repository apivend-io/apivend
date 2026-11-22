{{- define "x402.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end }}

{{- define "x402.labels" -}}
app.kubernetes.io/name: x402
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* Pod template labels: without helm.sh/chart, so an upgrade that changes
only the chart's version (GitOps tools add the commit to it) does not
restart every pod. */}}
{{- define "x402.podLabels" -}}
app.kubernetes.io/name: x402
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "x402.redisEnv" -}}
- name: X402_REDIS
{{- if .Values.redis.existingSecret.name }}
  valueFrom:
    secretKeyRef:
      name: {{ .Values.redis.existingSecret.name }}
      key: {{ .Values.redis.existingSecret.key }}
{{- else }}
  value: {{ required "set redis.url or redis.existingSecret.name" .Values.redis.url | quote }}
{{- end }}
{{- end }}

{{- define "x402.rpcEnv" -}}
{{- range $i, $r := .Values.rpc }}
- name: X402_RPC_{{ $i }}
  valueFrom:
    secretKeyRef:
      name: {{ $r.secretName }}
      key: {{ $r.secretKey }}
{{- end }}
{{- end }}

{{- define "x402.rpcArgs" -}}
{{- range $i, $r := .Values.rpc }}
- -rpc={{ $r.network }}=$(X402_RPC_{{ $i }})
{{- end }}
{{- end }}

{{- define "x402.podSecurity" -}}
securityContext:
  runAsNonRoot: true
  # Numeric, so the kubelet can verify runAsNonRoot whatever USER the image names.
  runAsUser: 65532
  runAsGroup: 65532
  seccompProfile:
    type: RuntimeDefault
{{- end }}

{{- define "x402.containerSecurity" -}}
securityContext:
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: true
  capabilities:
    drop: ["ALL"]
{{- end }}
