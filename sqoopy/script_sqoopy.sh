chemin_script=`realpath $0`
chemin=`dirname ${chemin_script}`
asterisk='*'

if [ "${DB_NAME}" == "" ]
then
  DB_NAME="CNET_EXP"
fi
if [ "${SQOOP_OPTS}" == "" ]
then
  SQOOP_OPTS=""
fi
echo "using the database [\$DB_NAME=${DB_NAME}]"
echo 
usage() {
	echo "$0 [nom_de_table]+"
    echo
    echo "The default database being targeted is CNET_EXP."
    echo "When one needs to target a different database as the datasource, then"
    echo "One needs to declare the env variable such as follow (for exemple) :"
    echo "\texport DB_NAME=GULPER_FICHIER_EXP."
    echo "Then proceed with the call to the script"
}

if [ -z "${password}" ]
 then
        read -s -p "$USER's password ?" password
fi





uitlisateur_kerberos=$USER
uitlisateur_kerberos+="@DEV_PREDIMED.CHUG.ALP"
echo "changeme" | kinit ${uitlisateur_kerberos}



DB_NAME=`echo ${DB_NAME} |tr '[:lower:]' '[:upper:]'`
DB_NAME_final=${DB_NAME}
DB_NAME+="_temp"
				beeline="beeline -u 'jdbc:hive2://yyxx.exploitation:10000/;principal=hive/yyxx.exploitation@blabla' -n ${USER} -p${password}"
				#eval $cmd
				echo "create database ${DB_NAME} if not exist"
				CREATE_DB="create database IF NOT EXISTS ${DB_NAME};"
				cmd="time ${beeline} --silent=true -e '${CREATE_DB}'"
				eval $cmd
				echo "create database ${DB_NAME_final} if not exist"
				CREATE_DB_f="create database IF NOT EXISTS ${DB_NAME_final};"
				cmd="time ${beeline} --silent=true -e  '${CREATE_DB_f}'"
				eval $cmd
 
#initialisation de la variables commandee avant boucle 
beeline="beeline -u 'jdbc:hive2://yyxx.exploitation:10000/${DB_NAME_final};principal=hive/yyxx.exploitation@blabla' -n ${USER} -p${password}"
cmd="time ${beeline} --silent=true"
  for TABLE_NAME in $*
  do
      	TABLE_NAME=`echo ${TABLE_NAME} |tr '[:lower:]' '[:upper:]'` 
        echo "drop table ${TABLE_NAME};"
        DROP_QUERY="drop table if exists ${TABLE_NAME};"
        cmd+=" -e '${DROP_QUERY}'"
  done
eval $cmd

cat ${chemin}/sqoop.sh | while read line
do
  #echo ${line} --username=${USER} --password=${password}
  eval time ${line} --username=${USER} --password=${password} >> temp
done
beeline="beeline -u 'jdbc:hive2://yyxx.exploitation:10000/;principal=hive/yyxx.exploitation@blabla' -n ${USER} -p${password}"
cmd_insert="time ${beeline} --silent=true"
cmd_create_parq="time ${beeline} --silent=true"
  for TABLE_NAME in $*
  do
				echo "changeme" | kinit ${uitlisateur_kerberos}
				TABLE_NAME=`echo ${TABLE_NAME} |tr '[:lower:]' '[:upper:]'`
				echo "CREATE TABLE ${DB_NAME_final}.${TABLE_NAME} LIKE ${DB_NAME}.${TABLE_NAME} STORED AS PARQUET;"
				create_parquet_table="CREATE TABLE ${DB_NAME_final}.${TABLE_NAME} LIKE ${DB_NAME}.${TABLE_NAME} STORED AS PARQUET;"
				cmd_create_parq+=" -e '${create_parquet_table}'"


				echo "INSERT INTO ${DB_NAME_final}.${TABLE_NAME} select * from ${DB_NAME}.${TABLE_NAME};"
				parquet_table="INSERT INTO ${DB_NAME_final}.${TABLE_NAME} select ${asterisk} from ${DB_NAME}.${TABLE_NAME};"
				cmd_insert+=" -e '${parquet_table}'"


				echo "ANALYZE TABLE ${DB_NAME_final}.${TABLE_NAME} COMPUTE STATISTICS FOR COLUMNS;"
				stat_table="ANALYZE TABLE ${DB_NAME_final}.${TABLE_NAME} COMPUTE STATISTICS FOR COLUMNS;"
				cmd_insert+=" -e '${stat_table}'"
  done

eval $cmd_create_parq
eval "$cmd_insert"

				echo "DROP DATABASE IF EXISTS ${DB_NAME} CASCADE;"
				DROP_DB="DROP DATABASE IF EXISTS ${DB_NAME} CASCADE;"
				cmd="time ${beeline} --silent=true -e '${DROP_DB}'"
				#echo $cmd
	eval $cmd
