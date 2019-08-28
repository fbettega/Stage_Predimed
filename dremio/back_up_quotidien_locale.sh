#!/bin/bash
# On peut envisager de faire tourner le script depuis une autre machine
cur_dir="/home/${LOGNAME}/"
pass_dremio=$(cat ${cur_dir}password | head -3 | tail -1) # lecture du mdp dremio

LANG=C DOW=$(date +"%a") #recuperation du jour de la semaine
localFolder="${cur_dir}dremio_backup_auto/${DOW}"



mkdir -p ${localFolder}
docker exec dremio sh -c "export PATH=$PATH:/opt/dremio/bin && mkdir -p /tmp/dremio_backup_auto/ && dremio-admin backup -u ${LOGNAME} -p ${pass_dremio} -d /tmp/dremio_backup_auto" 2>&1 > /tmp/backup_dremio_auto.txt #back up en lui même et ecriture des logs dans un fichier temporaire pour récupérer le nom du back_up
last_line=$(tail -1 /tmp/backup_dremio_auto.txt)
rm /tmp/backup_dremio_auto.txt
b=${last_line%%,*}
nom_backup=${b#*auto/} #nom du back_up
docker exec dremio sh -c "tar jcf /tmp/dremio_backup_auto/${nom_backup}.tbz2 -C /tmp/dremio_backup_auto ${nom_backup}" #compression du backup
docker cp dremio:/tmp/dremio_backup_auto/${nom_backup}.tbz2  "${localFolder}" #transfert vers profront On peut envisager de stocké les backup quotidiens dans un espace dédié
docker exec dremio sh -c "rm -r /tmp/dremio_backup_auto/" #suppressions du dossier de backup sur le container dremio

ssh ${LOGNAME}@proback89 "rm -r /opt/dremio_backup_auto/quotidien/${DOW}"

eval "scp -r ./dremio_backup_auto/${DOW} ${LOGNAME}@xxyy:/opt/dremio_backup_auto/quotidien"

rm -r "${localFolder}"
