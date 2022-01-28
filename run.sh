source ./env # from env import TOKEN_FILE, CREDS


if ! test -e $TOKEN_FILE; then
  printf 'You must set $TOKEN_FILE.\n'
  exit 1
fi


if ! python3 tokens/sheets.py $CREDS $TOKEN_FILE; then
  printf 'Need to reauthenticate.\n'
  ./get_token.sh
fi

printf "Running script. Log-level: $1\n"
python3 -m covid_school $@
