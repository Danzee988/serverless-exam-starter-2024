import { APIGatewayProxyHandlerV2 } from "aws-lambda";
import { Movie, MovieAward } from "../shared/types";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  QueryCommand,
  QueryCommandInput,
  GetCommand,
} from "@aws-sdk/lib-dynamodb";
import Ajv from "ajv";
import schema from "../shared/types.schema.json";

type ResponseBody = {
  data: {
    movieAward: MovieAward;
  };
};

const ajv = new Ajv({ coerceTypes: true });
const isValidQueryParams = ajv.compile(
  schema.definitions["MovieQueryParams"] || {}
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

        const getCommandOutput = await ddbDocClient.send(
            new GetCommand({
                TableName: process.env.MOVIES_TABLE_NAME,
                Key: { movieId: movieId},
            })
        );
        if (!getCommandOutput.Item) {
            return {
                statusCode: 404,
                headers: {
                "content-type": "application/json",
                },
                body: JSON.stringify({ Message: "Invalid movie Id" }),
            };
        }
        const body: ResponseBody = {
            data: { movieAward: getCommandOutput.Item as MovieAward },
        };

        const queryParams = event.queryStringParameters;
        if (isValidQueryParams(queryParams)) {
          let queryCommandInput: QueryCommandInput = {
            TableName: process.env.AWARDS_TABLE_NAME,
          };
          queryCommandInput = {
            ...queryCommandInput,
            KeyConditionExpression: "movieId = :m",
            ExpressionAttributeValues: {
              ":m": movieId,
            },
          };
          const queryCommandOutput = await ddbDocClient.send(
            new QueryCommand(queryCommandInput)
          );
          if (queryCommandOutput.Items && queryCommandOutput.Items.length > 0) {
            body.data.movieAward = queryCommandOutput.Items[0] as MovieAward;
          } else {
            return {
              statusCode: 404,
              headers: {
                "content-type": "application/json",
              },
              body: JSON.stringify({ Message: "No awards found for the movie" }),
            };
          }
        }
        return {
          statusCode: 200,
          headers: {
            "content-type": "application/json",
          },
          body: JSON.stringify(body),
        };
    } catch (error: any) {
        console.log(JSON.stringify(error));
        return {
          statusCode: 500,
          headers: {
            "content-type": "application/json",
          },
          body: JSON.stringify({ error}),
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
