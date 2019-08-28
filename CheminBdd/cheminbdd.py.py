# -*- coding: utf-8 -*-
'''
Usage: cheminbdd.py [--mode] [--ensemble] [--ensemb_tables]
[--table] 

cheminbdd: Recherche le plus court chemin entre deux tables de la base de données CNET_EXP

Arguments:
	mode		Choix du mode path, voisin ou join
	ensemble	effectuer la recherche sur toutes les tables de la base ou un sous ensemble [optionnel]
	ensemb_tables        Sous ensemble de tables séparées par des espaces sur lequels on effectue la recherche [optionnel]
	tables		Liste de table et colonne  spérarer par des espaces a relier
'''
"""
Created on Wed Jun  5 09:06:07 2019

@author: AdminFBe
"""

import argparse
import pyodbc
import pandas as pd
from dijkstar import Graph, find_path
from pathlib import Path



def uniquel(mylist):
    used = set()
    a=[x for x in mylist if x not in used and (used.add(x) or True)]
    return a

def graph_gen_col(df_edge): #fonction créant le graphique à partir du tableau des contraintes relationnel
    graph = Graph()
    for x in df_edge.index:       
        graph.add_edge(df_edge.loc[x,'referenced_table_col'], df_edge.loc[x,'parent_table_col'], {'cost': 1}) #ajoute le lien entre les 2 tables relié dans un sens
        graph.add_edge(df_edge.loc[x,'parent_table_col'],df_edge.loc[x,'referenced_table_col'],  {'cost': 1}) #ajoute le lien entre les 2 tables relié dans l'autre sens
        if df_edge.loc[x,'referenced_column_id']==1: # col id  = 1 correspond a une clef primaire
            a1=df_edge[(df_edge['table_FK_id']==df_edge.loc[x,'table_FK_id'])]['referenced_column_id'].unique().tolist()  #crée des liens entre la clef primaire d'une table et toutes les colones ayant une contrainte référentiel avec cette table
            a2=df_edge[(df_edge['tablePK_id']==df_edge.loc[x,'table_FK_id'])]['parent_colum_id'].unique().tolist()
            a=sorted(uniquel(a1+a2))[1:]     
            if a:
                for i in a:
                    graph.add_edge(df_edge.loc[x,'referenced_table_col'],int(df_edge.loc[x,'table_FK_id'].astype(str)+"0000"+str(i)),  {'cost': 1}) #ajoute les edges avec la lsite crée plus haut
                    graph.add_edge(int(df_edge.loc[x,'table_FK_id'].astype(str)+"0000"+str(i)),df_edge.loc[x,'referenced_table_col'],  {'cost': 1}) 
        if df_edge.loc[x,'parent_colum_id']==1:
            b1=df_edge[(df_edge['tablePK_id']==df_edge.loc[x,'tablePK_id'])]['referenced_column_id'].unique().tolist()
            b2=df_edge[(df_edge['table_FK_id']==df_edge.loc[x,'tablePK_id'])]['parent_colum_id'].unique().tolist()
            b=sorted(uniquel(b1+b2))[1:]             
            if b:
                for i in b:
                    graph.add_edge(df_edge.loc[x,'parent_table_col'],int(df_edge.loc[x,'tablePK_id'].astype(str)+"0000"+str(i)),  {'cost': 1})
                    graph.add_edge(int(df_edge.loc[x,'tablePK_id'].astype(str)+"0000"+str(i)),df_edge.loc[x,'parent_table_col'],  {'cost': 1})
    return graph



def graph_gen(df_edge): #génère un graph simple des liens entre les tables sans tenir compte des colones
    graph = Graph()
    for x in df_edge.index:
        graph.add_edge(df_edge.loc[x,'table_FK_id'], df_edge.loc[x,'tablePK_id'], {'cost': 1})
        graph.add_edge(df_edge.loc[x,'tablePK_id'],df_edge.loc[x,'table_FK_id'],  {'cost': 1})
    return graph



def voisins(table_dep,table_lien): #permet de créer la dataframe des contraintes référentiels
    vois1=table_lien[table_lien['table']==table_dep]
    vois2=table_lien[table_lien['referenced_table']==table_dep]
    vois2=vois2[['referenced_table','referenced_column','table','column']]
    vois2 .rename(columns={'referenced_table':'table_depart','referenced_column':'colonne_depart','table':'table_voisine','column':'colonne_voisine'},inplace=True)
    vois1=vois1[['table','column','referenced_table','referenced_column']]
    vois1.rename(columns={'table':'table_depart','column':'colonne_depart','referenced_table':'table_voisine','referenced_column':'colonne_voisine'},inplace=True)
    vois=pd.concat([vois1, vois2])
    return vois

def nom_to_id(list_nom,list_nom_col=[],table=False): #fait la conversions du noms d'une table ou d'une table et d'une colones vers leur id dans la dataframe voins
    list_id=[]
    if table==True:
        for nom in list_nom:
            if nom in nom_table.name.values:
                list_id.append(int(str(nom_table[nom_table['name']== nom]['id'].values[0]).rsplit("0000", 1)[0]))
        return list_id
    else:
        for nom in list_nom:
            if not list_nom_col:
                if len(uniquel(nom_table[nom_table['name']== nom]['col_name'].values))==1:
                    list_nom_col=uniquel(nom_table[nom_table['name']== nom]['col_name'].values)
                else:    
                     print(("choisir un noms de colones parmis :", uniquel(nom_table[nom_table['name']== nom]['col_name'].values)))
                     return None
            for col in list_nom_col:
                     if nom in nom_table.name.values:
                         if col in nom_table.col_name.values:
                             list_id.append(nom_table[(nom_table['name']== nom)&(nom_table['col_name']== col)]['id'].values[0])
    if len(list_id)==0:
        print("Nom de colone ou de table invalide")
    return list_id


def id_to_nom(id_list):
    list_nom=[]
    for iid in id_list:
        list_nom.append(nom_table[nom_table['id']== iid][['name','col_name']].values.tolist()[0])
    return list_nom
    
def path_to_nom(path):
    return id_to_nom(path[0])




conn_mssql = pyodbc.connect('Driver={SQL Server};' 'Server=proback82;''Database=CNET_EXP;''Trusted_Connection=yes;')


#définition des arguments possible
parser = argparse.ArgumentParser()
parser.add_argument("--mode", type=str,
                    help="choix d'utilisation du script", choices=["path","join","contrainte"], required=True)
parser.add_argument("--ensemble", type=int, nargs='?',const=0, default=0,
                    help="toute les tables 0 ou une sous partie donnée dans l'argument --ensemb_tables 1", choices=[0, 1])
parser.add_argument('--ensemb_tables', nargs='+', help='Sous ensemble de table parmis lesquels le chemin est cherché séparées par des espaces')
parser.add_argument('--tables', nargs='+',type=str, help="si utilisé en mode path ou join correspond aux tables  et colones de départ et d'arrivé entrée sous la forme tables_depart colonne_depart table_arrivee colonne_arrive séparées par des espaces\n si utilisé en mode voisin correspond à la table dont on recherche les voisins", required=True)
args = parser.parse_args()

voisin_df_temp=None

#récupérations sur la base de données des contraintes référentiels
SQL_Query = pd.read_sql_query('SELECT tab1.name AS [table],tab1.object_id as tablePK_id,tab2.name AS [referenced_table],tab2.object_id as table_FK_id, col1.name AS [column], parent_column_id as parent_colum_id,  col2.name AS [referenced_column], referenced_column_id as referenced_column_id FROM sys.foreign_key_columns fkc INNER JOIN sys.objects obj ON obj.object_id = fkc.constraint_object_id INNER JOIN sys.objects obj2 on obj2.object_id = fkc.parent_object_id INNER JOIN sys.tables tab1 ON tab1.object_id = fkc.parent_object_id INNER JOIN sys.columns col1 ON col1.column_id = parent_column_id AND col1.object_id = tab1.object_id INNER JOIN sys.tables tab2 ON tab2.object_id = fkc.referenced_object_id INNER JOIN sys.columns col2 ON col2.column_id = referenced_column_id AND col2.object_id = tab2.object_id;', conn_mssql)
voisins_df = pd.DataFrame(SQL_Query, columns=['table','tablePK_id','referenced_table','table_FK_id','column','parent_colum_id','referenced_column','referenced_column_id'])
voisins_df['parent_table_col']=(voisins_df['tablePK_id'].astype(str)+"0000"+voisins_df['parent_colum_id'].astype(str)).astype('int64')
voisins_df['referenced_table_col']=(voisins_df['table_FK_id'].astype(str)+"0000"+voisins_df['referenced_column_id'].astype(str)).astype('int64')
nom_table=pd.DataFrame({'name':pd.concat([voisins_df['table'], voisins_df['referenced_table']]),'col_name':pd.concat([voisins_df['column'], voisins_df['referenced_column']]),'id':pd.concat([voisins_df['parent_table_col'], voisins_df['referenced_table_col']])})

if 1 == args.ensemble:#Si spécifié utilisations d'un sous ensemble des tables de la base de donnée
    hive_table=[x.upper() for x in args.ensemb_tables]
    hive_table_index=nom_to_id(hive_table,table=True)
    voisins_df=voisins_df[voisins_df.referenced_table_col.isin(hive_table_index) & (voisins_df.parent_table_col.isin(hive_table_index))]
        
graph_tot_col=graph_gen_col(voisins_df)
graph_tot_table=graph_gen(voisins_df)

def path_col_table(graph_col,graph_tab,dep_table,dep_col,arr_table,arr_col,cost_func=[],voisin_df_temp=voisins_df): #fonction qui recherche si un chemins existe entre un points de départ et d'arrivé dans un graph donnée
    dep=nom_to_id(dep_table,dep_col)[0]
    arr=nom_to_id(arr_table,arr_col)[0]
    cost_func = lambda u, v, e, prev_e: e['cost']

    try:        
        path=find_path(graph_col, dep, arr, cost_func=cost_func)
        if path: # retrait des tables vide du chemins possible 
            tab_vide=[]
            nom_list=id_to_nom(path[0])
            requete="select count( " +nom_list[0][1]+ " )" + " from " + nom_list[0][0] + " table0" 
            pd.read_sql_query(requete, conn_mssql).values.tolist()[0][0]
            for i in range(len(nom_list)):
                requete="select count( " +nom_list[i][1]+ " )" + " from " + nom_list[i][0] + " table0" 
                compte=pd.read_sql_query(requete, conn_mssql).values.tolist()[0][0]
                if compte ==0:
                    tab_vide.append(nom_list[i])    
            index_table_vide=[]
            if len(tab_vide)==0:
                return path
            else:
                for i in range(len(tab_vide)):
                    liste_table_vide=nom_to_id([tab_vide[i][0]] ,[tab_vide[i][1]])[0]
                    index_table_vide.append(voisin_df_temp.index[(voisin_df_temp['parent_table_col']==liste_table_vide)|(voisin_df_temp['referenced_table_col']==liste_table_vide)].tolist()[0])
                index_table_vide=uniquel(index_table_vide)    
                index_table_vide.sort()    
                for i in index_table_vide:
                    voisins_df_temp=voisin_df_temp.drop(voisin_df_temp.index[i])
                graph_tot_col_temp=graph_gen_col(voisins_df_temp)    
                path_col_table(graph_tot_col_temp,graph_tab,dep_table,dep_col,arr_table,arr_col,cost_func=[],voisin_df_temp=voisin_df_temp)   
    except : #si aucun chemins n'existe en utilisant les colones vérifié on vérifie si il existe un chemin entre les tables par d'autre colonnes
        print("\n Il a été impossible de trouver un chemin entre les tables spécifier via les colonnes spécifié mais il existe un chemin entre ces tables qui est le suivant \n")
        dep2=int(str(dep).rsplit("0000", 1)[0])
        arr2=int(str(arr).rsplit("0000", 1)[0])
        path_tab=find_path(graph_tab, dep2, arr2, cost_func=cost_func)
        print(path_tab)
        return path_tab
    

cost_func = lambda u, v, e, prev_e: e['cost']
 



def jointure(dep_table,dep_col,arr_table,arr_col,graph_col): # crée la jointure entre 2 tables a partir du chemins
    path=path_col_table(graph_col,graph_tot_table,dep_table,dep_col,arr_table,arr_col)
    nom_list=id_to_nom(path[0])
    requete="select * from " + nom_list[0][0] + " table0" 
    compte_tab=1
    for i in range(len(nom_list)):
        if i >= 1:
            if nom_list[i][0]!=nom_list[i-1][0]:
                jointure=" INNER JOIN "   + nom_list[i][0] + " table" +str(compte_tab) + " ON " +" table"+ str(compte_tab)+"." +nom_list[i][1]  +"="+ "table" + str(compte_tab-1)+"." + nom_list[i-1][1] 
                requete=requete+jointure
                compte_tab=compte_tab+1
    print("Pour aller de " + nom_list[0][0] +" à " + nom_list[-1][0] + " la requetes de jointure est la suivante :\n " )
    print(requete)




tables_script=[x.upper() for x in args.tables]
print(tables_script)
if "path" == args.mode:
    path_def=path_col_table(graph_tot_col,graph_tot_table,[tables_script[0]],[tables_script[1]],[tables_script[2]],[tables_script[3]] )
    print("Le chemin passe par les ID de tables suivante :\n")
    print(path_def)
    print("\n")
    print("Ces ID correspondent aux tables suivante :\n")
    print("\n")
    print(path_to_nom(path_def))    

if "join" == args.mode:    
    print(jointure([tables_script[0]],[tables_script[1]],[tables_script[2]],[tables_script[3]],graph_col=graph_tot_col))

if "contrainte" == args.mode:  
    df_export_voisin=voisins(tables_script[0],voisins_df)
    fpath = (Path.cwd() / 'contrainte' ).with_suffix('.csv')
    print("la liste des tables liée par une contrainte référentiel a était importer dans un CSV du nom de  contrainte il est situé dans la directory : \n" ,fpath)
    df_export_voisin.to_csv(fpath, sep=';', encoding='utf-8')    


