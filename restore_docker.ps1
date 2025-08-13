#복원 방법
#docker desktop만 열어둠. up실행x. docker-compose down -v 실행 후
#이 스크립트의 경로 복사하여 터미널에 붙여넣고 완료하면 docker-compose up -d

# 이미지 복원
# Write-Host "Docker restore images..."
# docker load -i .\all_images.tar

# Write-Host "🔄 Docker restore from volumes.zip..."

# # 압축 해제 경로
# $backupFolder = "$(PWD)\volumes"
# $zipPath = "$(PWD)\volumes.zip"

# # volumes.zip이 존재하면 압축 해제
# if (Test-Path $zipPath) {
#     if (Test-Path $backupFolder) {
#         Remove-Item $backupFolder -Recurse -Force
#     }
#     Expand-Archive -Path $zipPath -DestinationPath $backupFolder -Force
#     Write-Host "volumes.zip Clear"
# } else {
#     Write-Host "volumes.zip Fail."
#     exit 1
# }

# # .tar.gz 파일을 이용한 복원
# $volumeFiles = Get-ChildItem -Path $backupFolder -Filter *.tar.gz

# foreach ($file in $volumeFiles) {
#     $volumeName = $file.Name -replace '\.tar\.gz$', ''
#     Write-Host "restore: $volumeName"

#     # 볼륨이 존재하지 않으면 생성
#     docker volume inspect $volumeName > $null 2>&1
#     if ($LASTEXITCODE -ne 0) {
#         docker volume create $volumeName | Out-Null
#     }

#     # 압축 해제 및 복원
#     docker run --rm `
#         -v "${volumeName}:/volume" `
#         -v "${backupFolder}:/backup" `
#         alpine sh -c "cd /volume && tar xzf /backup/$($file.Name)"
# }

# Write-Host "All clear!"
