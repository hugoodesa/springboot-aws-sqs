#!/bin/bash
set -e

echo "=== Configurando Filas SQS no LocalStack ==="

# 1. Cria a Dead Letter Queue (DLQ)
awslocal sqs create-queue \
  --queue-name events-hub-dlq \
  --region us-east-1

# Obtém o ARN da DLQ criada
DLQ_ARN=$(awslocal sqs get-queue-attributes \
  --queue-url http://localhost:4566/000000000000/events-hub-dlq \
  --attribute-names QueueArn \
  --query 'Attributes.QueueArn' \
  --output text \
  --region us-east-1)

echo "DLQ configurada com ARN: ${DLQ_ARN}"

# 2. Define a Redrive Policy (maxReceiveCount = 3)
REDRIVE_POLICY="{\"deadLetterTargetArn\":\"${DLQ_ARN}\",\"maxReceiveCount\":\"3\"}"

# 3. Cria a Fila Principal vinculada à DLQ com VisibilityTimeout de 30 segundos
awslocal sqs create-queue \
  --queue-name events-hub-queue \
  --attributes "{\"RedrivePolicy\":\"$(echo ${REDRIVE_POLICY} | sed 's/\"/\\\"/g')\",\"VisibilityTimeout\":\"30\"}" \
  --region us-east-1

echo "Fila principal 'events-hub-queue' criada e conectada com sucesso à DLQ 'events-hub-dlq'!"
