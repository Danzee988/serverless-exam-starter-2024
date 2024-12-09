import { APIGatewayProxyHandlerV2 } from "aws-lambda";
import { MovieAwardQueryParams } from "../shared/types";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  QueryCommand,
  QueryCommandInput,
} from "@aws-sdk/lib-dynamodb";
import Ajv from "ajv";
import schema from "../shared/types.schema.json";

const ajv = new Ajv({ coerceTypes: true });
const isValidQueryParams = ajv.compile(
  schema.definitions["MovieAwardQueryParams"] || {}
);

const ddbDocClient = createDDbDocClient();

export const handler: APIGatewayProxyHandlerV2 = async (event, context) => {
    try {
        console.log("Event: ", JSON.stringify(event));
        const parameters = event?.pathParameters;
        const movieId = parameters?.movieId
        ? parseInt(parameters.movieId)
        : undefined;

        const movieAward = parameters?.movieAward
        ? parameters.movieAward
        : undefined;

        if (!movieId) {
            return {
              statusCode: 404,
              headers: {
                "content-type": "application/json",
              },
              body: JSON.stringify({ Message: "Missing movie Id" }),
            };
          } 

        if (!movieAward) {
                return {
                statusCode: 404,
                headers: {
                    "content-type": "application/json",
                },
                body: JSON.stringify({ Message: "Missing movie Award" }),
                };
            }

            const queryParams = event.queryStringParameters;
            if (queryParams && !isValidQueryParams(queryParams)) {
              return {
                statusCode: 500,
                headers: {
                  "content-type": "application/json",
                },
                body: JSON.stringify({
                  message: `Incorrect type. Must match Query parameters schema`,
                  schema: schema.definitions["MovieAwardQueryParams"],
                }),
              };
            }

            let commandInput: QueryCommandInput = {
                TableName: process.env.AWARDS_TABLE_NAME,
              };

              if (queryParams) {
                if ("awardBody" in queryParams) {
                  commandInput = {
                    ...commandInput,
                    IndexName: "roleIx",
                    KeyConditionExpression: "movieId = :m and begins_with(awardBody, :r) ",
                    ExpressionAttributeValues: {
                      ":m": movieId,
                      ":r": queryParams.awardBody,
                    },
                  };
                }
              } else {
                commandInput = {
                  ...commandInput,
                  KeyConditionExpression: "movieId = :m",
                  ExpressionAttributeValues: {
                    ":m": movieId,
                  },
                };
              }

              const commandOutput = await ddbDocClient.send(
                new QueryCommand(commandInput)
              );

              return {
                statusCode: 200,
                headers: {
                  "content-type": "application/json",
                },
                body: JSON.stringify({
                  data: commandOutput.Items,
                }),
              };
            } catch (error: any) {
              console.log(JSON.stringify(error));
              return {
                statusCode: 500,
                headers: {
                  "content-type": "application/json",
                },
                body: JSON.stringify({ error }),
              };
            }
};

function createDDbDocClient() {
    const ddbClient = new DynamoDBClient({ region: process.env.REGION });
    const marshallOptions = {
      convertEmptyValues: true,
      removeUndefinedValues: true,
      convertClassInstanceToMap: true,
    };
    const unmarshallOptions = {
      wrapNumbers: false,
    };
    const translateConfig = { marshallOptions, unmarshallOptions };
    return DynamoDBDocumentClient.from(ddbClient, translateConfig);
  }
