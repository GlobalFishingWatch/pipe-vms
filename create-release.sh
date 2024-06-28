#!/bin/bash

ARGS=( \
  APP \
  VERSION \
)

################################################################################
# Validate and extract arguments
################################################################################
display_usage() {
  echo -e "\nUsage:\n${0} ${ARGS[*]}\n"
}

if [[ $# -ne ${#ARGS[@]} ]]
then
    display_usage
    exit 1
fi


ARG_VALUES=("$@")
PARAMS=()
for index in ${!ARGS[*]}; do
  declare "${ARGS[$index]}"="${ARG_VALUES[$index]}"
done

branch_name=$(git symbolic-ref -q HEAD)
branch_name=${branch_name##refs/heads/}
branch_name=${branch_name:-HEAD}


if [ $branch_name != "main" ]
then
echo "You must create release from main branch (current: $branch_name)"
exit 1
fi


if [ ! -d "./packages/$APP" ]; then
  echo "$APP does not exist. Apps available:"
  for dir in packages/*/; do basename "$dir"; done
  exit 1
fi

echo "Creating release of $APP for version $VERSION"
echo $VERSION $APP
npx nx release changelog \
  -i "all" \
  -p $APP \
  --from develop \
  ${VERSION} 
