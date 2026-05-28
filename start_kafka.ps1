if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Error "Docker is not installed or not on PATH. Install Docker Desktop and start it first."
    exit 1
}

Write-Host "Starting Kafka stack with docker compose..."
docker compose up -d

Write-Host "Waiting for Kafka broker on localhost:9092..."
for ($i = 0; $i -lt 12; $i++) {
    $result = Test-NetConnection -ComputerName localhost -Port 9092 -WarningAction SilentlyContinue
    if ($result.TcpTestSucceeded) {
        Write-Host "✅ Kafka broker is available at localhost:9092"
        exit 0
    }
    Start-Sleep -Seconds 5
}

Write-Error "Kafka broker did not become available on localhost:9092. Check Docker logs with: docker compose logs kafka"
exit 1
