module Test.Main where

import Prelude
import AWS.Dynamo (StreamPayload)
import AWS.Dynamo.Classes (class FromDynamo, dynamoReadGeneric)
import Control.Monad.Aff.AVar (AVAR)
import Control.Monad.Eff (Eff)
import Control.Monad.Eff.Console (CONSOLE)
import Control.Monad.Except (Except, runExcept, withExcept)
import Data.Either (Either, either)
import Data.Foreign (F)
import Data.Foreign.Class (readJSON)
import Data.Generic.Rep (class Generic)
import Test.Unit (test, Test, failure, success)
import Test.Unit.Console (TESTOUTPUT)
import Test.Unit.Main (runTest)


type TestEffs = (console :: CONSOLE, testOutput :: TESTOUTPUT, avar :: AVAR)

newtype TestRecord = TestRecord
  { id :: String
  , retrievetime :: Int
  , user :: String
  }
derive instance genericTestRecord :: Generic TestRecord _

instance fromDynamoTestRecord :: FromDynamo TestRecord where
    dynamoRead = dynamoReadGeneric

payload :: String
payload = """
{
    "Records": [
        {
            "eventID": "7de3041dd709b024af6f29e4fa13d34c",
            "eventName": "INSERT",
            "eventVersion": "1.1",
            "eventSource": "aws:dynamodb",
            "awsRegion": "us-west-2",
            "dynamodb": {
                "ApproximateCreationDateTime": 1479499740,
                "Keys": {
                    "id": {
                        "S": "foo"
                    },
                    "retrievetime": {
                        "N": "100000"
                    }
                },
                "NewImage": {
                    "id": {
                        "S": "foo"
                    },
                    "retrievetime": {
                        "N": "100000"
                    },
                    "user": {
                        "S": "John Doe"
                    }
                },
                "SequenceNumber": "13021600000000001596893679",
                "SizeBytes": 112,
                "StreamViewType": "NEW_IMAGE"
            },
            "eventSourceARN": "arn:aws:dynamodb:us-east-1:123456789012:table/BarkTable/stream/2016-11-16T20:42:48.104"
        }
    ]
}
"""

main :: Eff TestEffs Unit
main = do
  runTest $ do
    test "Stream payload for TestRecord can be deserialized" $ do
      assertExcept (readJSON payload :: F (StreamPayload TestRecord))


assertExcept :: forall e a eff. (Show e) => Except e a -> Test eff
assertExcept result = assertEither $ runExcept $ withExcept show result

assertEither :: forall a eff. Either String a -> Test eff
assertEither = either failure (const success)

