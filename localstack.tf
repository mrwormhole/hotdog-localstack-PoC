provider "aws" {
  region                      = "ap-southeast-2"
  access_key                  = "fake"
  secret_key                  = "fake"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    dynamodb = "http://localhost:4566"
    lambda   = "http://localhost:4566"
    kinesis  = "http://localhost:4566"
  }
}

// DYNAMODB TABLES
resource "aws_dynamodb_table" "dogs" {
  name           = "dogs"
  read_capacity  = "20"
  write_capacity = "20"
  hash_key       = "ID"

  attribute {
    name = "ID"
    type = "S"
  }
}

// KINESIS STREAMS
resource "aws_kinesis_stream" "caught_dogs_stream" {
  name = "caughtDogs"
  shard_count = 1
  retention_period = 30

  shard_level_metrics = [
    "IncomingBytes",
    "OutgoingBytes",
  ]
}

resource "aws_kinesis_stream" "hot_dogs_stream" {
  name = "hotDogs"
  shard_count = 1
  retention_period = 30

  shard_level_metrics = [
    "IncomingBytes",
    "OutgoingBytes",
  ]
}

resource "aws_kinesis_stream" "eaten_hot_dogs_stream" {
  name="eatenHotDogs"
  shard_count = 1
  retention_period = 30

  shard_level_metrics = [
    "IncomingBytes",
    "OutgoingBytes",
  ]
}

// LAMBDA FUNCTIONS
resource "aws_lambda_function" "dog_catcher_lambda" {
  function_name = "dogCatcher"
  filename      = "dogCatcher.zip"
  handler       = "main"
  role          = "fake_role"
  runtime       = "go1.x"
  timeout       = 5
  memory_size   = 128
}

resource "aws_lambda_function" "dog_processor_lambda" {
  function_name = "dogProcessor"
  filename      = "dogProcessor.zip"
  handler       = "main"
  role          = "fake_role"
  runtime       = "go1.x"
  timeout       = 5
  memory_size   = 128
}

resource "aws_lambda_function" "hot_dog_despatcher_lambda" {
  function_name = "hotDogDespatcher"
  filename      = "hotDogDespatcher.zip"
  handler       = "main"
  role          = "fake_role"
  runtime       = "go1.x"
  timeout       = 5
  memory_size   = 128
}

// LAMBDA TRIGGERS
resource "aws_lambda_event_source_mapping" "dog_processor_trigger" {
  event_source_arn              = aws_kinesis_stream.caught_dogs_stream.arn
  function_name                 = "dogProcessor"
  batch_size                    = 1
  starting_position             = "LATEST"
  enabled                       = true
  maximum_record_age_in_seconds = 604800
}

resource "aws_lambda_event_source_mapping" "dog_processor_trigger_2" {
  event_source_arn              = aws_kinesis_stream.eaten_hot_dogs_stream.arn
  function_name                 = "dogProcessor"
  batch_size                    = 1
  starting_position             = "LATEST"
  enabled                       = true
  maximum_record_age_in_seconds = 604800
}

resource "aws_lambda_event_source_mapping" "hot_dog_despatcher_trigger" {
  event_source_arn = aws_kinesis_stream.hot_dogs_stream.arn
  function_name = "hotDogDespatcher"
  batch_size = 1
  starting_position = "LATEST"
  enabled = true
  maximum_record_age_in_seconds = 604800
}