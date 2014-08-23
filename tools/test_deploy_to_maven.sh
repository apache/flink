
echo "Call this in the tools/ directory!"
sleep 2
export TRAVIS_JOB_NUMBER="75.6"
export TRAVIS_PULL_REQUEST="false"

cd ..

./tools/deploy_to_maven.sh
