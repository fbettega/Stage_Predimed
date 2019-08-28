#!/bin/bash
mkdir -p /tmp/DossierDockerfile
cur_dir="/home/${LOGNAME}/"
pass_git=$(cat ${cur_dir}password | head -2 | tail -1) # lecture du mdp git
pass_dremio=$(cat ${cur_dir}password | head -3 | tail -1) # lecture du mdp dremio
repository="http://${LOGNAME}:${pass_git}@xxyy.exploitation/PREDIMED/dremio/backup_restore.git" #creation d'un nouveau repo
localFolder="${cur_dir}restoration_git"
git_folder="/backups_auto"
git clone "$repository" "$localFolder"
file_path=`ls "${localFolder}${git_folder}/back_up_mensuelle"/*`
file_name=${file_path##*/}
scp ${file_path} /tmp/DossierDockerfile/
echo -e "From dremio/dremio-oss\n RUN mkdir /opt/dremio/data/db\n RUN mkdir -p /tmp/dremio_backup/\n ADD ${file_name} /tmp/dremio_backup/\n RUN ls /tmp/dremio_backup/\n RUN /opt/dremio/bin/dremio-admin restore -d /tmp/dremio_backup/${file_name%.tbz2} -r\n USER root\n RUN rm -rf /tmp/dremio_backup/" > /tmp/DossierDockerfile/Dockerfile
cat /tmp/DossierDockerfile/Dockerfile
rm -rf "${localFolder}" # suppression du repo
docker build --no-cache -f /tmp/DossierDockerfile/Dockerfile -t dremio_backup /tmp/DossierDockerfile/
rm -r /tmp/DossierDockerfile/
docker run -d --name=dremio_restored -p 9018:9017 -p 31041:31040 -p 45639:45638 dremio_backup #version restaurant une instance parréllèle de dremio avec port mappé à +1 pour la version rééel voir le commentaire ci dessous

