# Import Table sur Predimed :
(En rouge les commandes)

## Note  
	La version actuelle du scripte est une version temporaire qui doit etre executé en partie sur pronlb28 et une autre partie sur simuback23
	la version definitive sera un script unique à executer sur pronlb28.

## premiere Partie sur Pronlb28 :
### prérequis  
#### Packages nécessaire à la créations de l'environnement sqoopy  
	docopt  
	pyyaml  
	texttable  
On peut exporter l'environnement en utilisant le fichier sqoopy.yml et la commande suivante
`conda env create -f sqoopy.yml`

Vous pouvez aussi créer l'envrionnement sqoopy avec les commandes suivantes :
`conda create -n sqoopy python=2.75 anaconda`  
`conda install docopt`  
`conda install pyyaml`  
`conda install -c conda-forge texttable`  
#### Fonction SQL utilisé par le scripte  
	Si la fonction dbo.describe n'existe plus utiliser le fichier describe_sql.txt pour la créer sur la base de données SQL server
### Execution du scripte	
Activation de l'environement python :  
`conda activate sqoopy`

Utilisation de sqoopy :  generate.py <host> <database> [--port=port] [--tables=tables] [--sqoop_options=sqoop_options] 
exempe :
`python generate.py server DB --port=1111 --tables='nom_de_tables,nom_de_tables2' `
 
### Transfert vers simuback23  
Récupéré le fichier sqoop.sh et le script script_sqoopy.sh,  les transférer vers simuback23 dans une même directory.

#### si besoin
executer
`dos2unix sqoop.sh`
`dos2unix script_sqoopy.sh`


### Execution du deuxiemes script
script_sqoopy.sh [nom_tables]
`sh script_sqoopy.sh nom_de_tables nom_de_tables2 `


####
Une liste des tables actuellement présente sur Predimed est conservé dans le fichier liste_tables_sqoopy.txt

#TO DO  
##Calcule des statistiques durant l'import  
##Ajout de la possibilité d'importer depuis des base Oracle ou Mysql  
##Execution en 1 script depuis Pronlb28  
##gestion des accents français dans les noms de colonne par exemple "GULP_UMG_STRUCTURE"
