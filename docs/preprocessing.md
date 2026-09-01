# Medicare Data Preprocessing

## Purpose

The preprocessing pipeline prepares Medicare provider-service data for integration and large-scale analytics using Apache Spark.

## Initial Cleaning

The pipeline:

- standardizes NPI as a numeric identifier;
- standardizes provider state abbreviations;
- trims categorical text fields;
- converts utilization, charge, and payment measures to numeric types;
- removes records missing critical NPI or HCPCS identifiers;
- preserves original Medicare measures for business interpretation.

## Missing Values and Imputation

Missing values were evaluated after the initial cleaning process.

The selected provider-service analytical dataset contained zero null values across the retained fields. Therefore, artificial imputation was not applied.

Critical identifiers such as NPI and HCPCS code were filtered when missing because imputing these identifiers would create unreliable provider or procedure relationships.

## Outlier Detection

The Interquartile Range (IQR) method was used to identify extreme values in:

- total beneficiaries;
- total services;
- average submitted charge;
- average Medicare allowed amount;
- average Medicare payment;
- average standardized Medicare payment.

The data contained substantial right-tail outliers. These observations were not deleted because unusually high utilization or payment values may represent legitimate Medicare activity.

## Outlier Treatment

Capped analytical features were created using the calculated IQR upper bounds.

The original values were preserved for reporting and interpretation. This allows the project to reduce the influence of extreme values during feature-based analysis without removing potentially meaningful Medicare activity.

## Normalization

Min-Max normalization was applied to capped numerical features.

The resulting analytical features range from 0 to 1. Original and capped values remain available alongside the normalized features.

## Categorical Encoding

PLACE_OF_SERVICE was encoded using Apache Spark StringIndexer.

The original categorical variable was retained while PLACE_OF_SERVICE_INDEX provides a numeric representation for downstream analytical processing.

## Binning

TOTAL_SERVICES was converted into quartile-based utilization categories:

- Low: 21 services or fewer
- Moderate: 22-43 services
- High: 44-117 services
- Very High: more than 117 services

The thresholds were derived from the observed distribution rather than arbitrary cutoffs.

## Validation

The preprocessing pipeline preserved all 9,781,673 cleaned provider-service records during feature engineering.

Validation confirmed:

- 9,781,673 cleaned input rows;
- 9,781,673 preprocessed output rows;
- zero rows lost during feature engineering;
- normalized analytical variables range from 0.0 to 1.0;
- original Medicare measures remain available;
- 26 columns are stored in the final preprocessing output.

## Output

The enhanced provider-service dataset is written to Parquet and partitioned by provider state.

Local output:

`data/processed/provider_services_preprocessed`

Parquet provides columnar storage, schema preservation, compression, and efficient column pruning for downstream Spark processing.
