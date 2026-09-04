variable "server_properties_path" {
  description = "Absolute host path to server.properties. Copy docker/etc/conf/server.properties.example to get started - see the root README."
  type        = string
}

variable "token_file_path" {
  description = "Absolute host path to token.txt. The server (re)generates this itself on every boot; if it doesn't exist yet on the host, `touch` it first so Docker bind-mounts a file rather than creating a directory."
  type        = string
}
