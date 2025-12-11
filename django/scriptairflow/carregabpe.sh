#!/usr/bin/bash
hostname
cd /application/home/ipm/

source .venv/bin/activate

export LD_LIBRARY_PATH=/opt/oracle/instantclient_21_12:$LD_LIBRARY_PATH

cod=`python manage.py carregabpe`

if ! [[ $cod =~ ^[0-9]+$ ]]
then
  exit 9
fi

# Verifica as outras condições
if [ $cod -eq 9 ]
then
  exit 9
fi

# Se não for 9, retorna o próprio código
exit $cod