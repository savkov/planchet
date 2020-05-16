if [ -z "$CC_TEST_REPORTER_ID" ]
then
  pytest .
else
  ./cc-test-reporter before-build
  pytest -v --cov-config .coveragerc --cov .
  coverage xml
  ./cc-test-reporter after-build -t coverage.py --exit-code $?
fi