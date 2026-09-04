output "container_name" {
  value = docker_container.uc_server.name
}

output "server_url" {
  value = "http://localhost:8080"
}
