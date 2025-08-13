#모든 볼륨 제거됨
Write-Host "Checking for dangling (unused) Docker volumes..."

# 고아 볼륨 목록 가져오기
$danglingVolumes = docker volume ls -f dangling=true --format "{{.Name}}"

if (-not $danglingVolumes) {
    Write-Host "No dangling volumes found. Nothing to clean."
} else {
    foreach ($vol in $danglingVolumes) {
        Write-Host "Removing dangling volume: $vol"
        docker volume rm $vol
    }
    Write-Host "Cleanup completed."
}
