cd /application/home/ipm/

source .venv/bin/activate
export LD_LIBRARY_PATH=/opt/oracle/instantclient_21_8
export DJANGO_SETTINGS_MODULE=ipm.settings
cod=$(python manage.py importacaoNFE_ipm)

# Verifica se o valor de cod é um número inteiro
if ! [[ $cod =~ ^[0-9]+$ ]]
then
  exit 9
fi

# Verifica as outras condições
if [ $cod -eq 1 ]
then
  exit 1
elif [ $cod -eq 9 ]
then
  exit 9
fi
