#백업 방법
#현재 작업영역의 volumes폴더와 all_images.tar이 있다면 삭제 후, 이 스크립트의 경로를 터미널에 복사붙여넣기
#완료 후, 그 두 파일을 옮긴다

# Write-Host "Docker backup start..."

# # 이미지 저장
# docker save -o all_images.tar (docker images --format "{{.Repository}}:{{.Tag}}" | Where-Object { $_ -ne "<none>:<none>" })

# $backupFolder = "$(PWD)\volumes"
# mkdir $backupFolder -Force

# # 모든 볼륨 백업
# $volumes = docker volume ls -q
# foreach ($volume in $volumes) {
#     $volumePath = Join-Path $backupFolder "$volume.tar.gz"
#     Write-Host "Backing up volume: $volume"
#     docker run --rm -v "${volume}:/volume" -v "${backupFolder}:/backup" alpine sh -c "cd /volume && tar czf /backup/$volume.tar.gz ."
# }

# # volumes 폴더를 ZIP으로 압축
# $zipPath = "$(PWD)\volumes.zip"
# if (Test-Path $zipPath) {
#     Remove-Item $zipPath -Force
# }
# Compress-Archive -Path "$backupFolder\*" -DestinationPath $zipPath

# Write-Host "Backup and compression complete: $zipPath"
