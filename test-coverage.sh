if [ -z "$CC_TEST_REPORTER_ID" ]
then
  pytest .
else
  curl -L -o cc-test-reporter https://codeclimate.com/downloads/test-reporter/test-reporter-latest-linux-amd64
  chmod +x cc-test-reporter
  cc-test-reporter before-build
  pytest -v --cov-config .coveragerc --cov .
  coverage xml
  cc-test-reporter after-build -t coverage.py --exit-code $?
fi