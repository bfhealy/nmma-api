## nmma-api
*!!! Work in progress !!!*

This is a simple API, which serves as a middleware and mini database (MongoDB) to run [NMMA](https://github.com/nuclear-multimessenger-astronomy/nmma) based on analysis requests coming from [SkyPortal](https://github.com/skyportal/skyportal). 

This service uses Expanse (and HPC cluster with CPU and GPU allocations, where we can submit jobs) to run the analyses.

It is composed of 3 distinct micro-services:
1. The API: receives the analysis requests submitted via SkyPortal, and creates an entry for each of them in the DB, containing the input data, parameters, and the webhook that will be used to post the results back to SkyPortal.
2. Submission queue: This queue, at a fixed rate, grabs analysis requests from the DB that haven't been submitted yet, and submits the jobs to Expanse.
3. Retrieval queue: This queue, at a fixed rate, checks if an analysis: finished running, has been running for too long, has failed... and uploads the results back to SkyPortal.

For the deployment, we rely on Heroku. When deployed on Heroku, parameters of the `config.yaml` file can be overwritten using environment variables. Here is an example:
Your config has:
```yaml
a:
  b: 'blabla'
```
you can set that value in Heroku's settings with `A_B=blabla` (capitalized, and `_` in between each keys). The data type of the variable will be inferred, and default to string if that is not possible.


