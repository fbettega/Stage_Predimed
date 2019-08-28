#Contenue du dossier
cheminbdd.py : Script
cheminbdd_env.yml : Permet de cr�er l'environnement avec les modules n�cessaire au script
	On peut exporter l'environnement en utilisant le fichier sqoopy.yml et la commande suivante
 : `conda env create -f cheminbdd_env.yml`
Sortie : exemple de sortie

#Fonctionnalit�
Permet de rechercher toutes les contraintes r�f�rentiels d'une table.
Recehrcher si il existe un chemins entre deux tables ou tables et colonnes de la base de donn�es sans passer par des tables vide.
G�n�re automatiquement la requ�tes de jointure si le chemins existe.


#Utilisation 
python cheminbdd.py --mode=voisin --tables DPLAN_INTERVENTION_PLANNING
python cheminbdd.py --mode=path --tables DPLAN_INTERVENTION_PLANNING PATIENT_ID  NOYAU_PATIENT PAT_ID
python cheminbdd.py --mode=join --tables DPLAN_INTERVENTION_PLANNING PATIENT_ID  NOYAU_PATIENT PAT_ID

