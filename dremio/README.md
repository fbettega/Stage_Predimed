# Script de backup automatique Dremio :
(En `rouge` le code et les commandes, --Commentaire)

## Liste des backups Dremio :
__Attention la restauration de dremio doit se faire dans la même version que celle où a été fait le backup__
En cas de changement de versions de Dremio il faut créer un nouveau dossier contenant les dernier backup auto de la versions précèdentes 
par exemple lorsque nous quitterons la version 3.1.1 il faudra créer le dossier backup_auto_3_1_1 en copiant le dossier backup_auto 


## Mise en place :
### Clef SSH :
Il est nécessaire pour que le scripte fonctionne que la clef SSH publiques de profront 203 soit autorisé sur proback89

exemple de tutoriel : https://www.cyberciti.biz/tips/ssh-public-key-based-authentication-how-to.html

### Mot de passe :
Il faut ajouter ses password pour dremio et Git_lab dans un document password sur profont 203
La premiere ligne de ce fichier doit etre le mot de passe git_lab
La seconde ligne de ce fichier doit etre le mot de passe dremio
__Il est conseillé de limiter les permissions de lectures pour ce fichier__
`chmod 600/home/User/password`

Pour modifier le chemin du fichier texte contenant le mot de passe ou les lignes lue il faut editer dremio_script.sh


###Optionnel
rendre les scripts executable
`chmod +x ~/back_up_quotidien_locale.sh`
`chmod +x ~/back_up_mensuelle.sh`
`chmod +x ~/back_up_hebdomadaire.sh`
`chmod +x ~/Script_restore_depuis_git.sh`
### Crontab
Il faut aller editer le fichier crontab pour définir à quelle fréquence le script sera executé via la commande :
`crontab -e`

Il faut allors rajouter la ligne suivante
`0 05 * * 1-5  bash ~/back_up_quotidien_locale.sh`
`0 04 * * 6 bash ~/back_up_hebdomadaire.sh`
`0 03 1 * * bash ~/back_up_mensuelle.sh`
 
les premiers caractère indique la frequence à laquel ont effectue l'execution du script ici quotidiennement à 5H00




