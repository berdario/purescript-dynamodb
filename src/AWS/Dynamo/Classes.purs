module AWS.Dynamo.Classes where

import Prelude
import Control.Alt ((<|>))
import Control.Monad.Except (mapExcept)
import Data.Bifunctor (lmap)
import Data.Either (Either(..))
import Data.Foreign (F, Foreign, ForeignError(..), fail, readArray, readString)
import Data.Foreign.Class (class IsForeign, read, readProp, readJSON)
import Data.Foreign.Generic.Classes (class GenericCountArgs, countArgs)
import Data.Generic.Rep (class Generic, Argument(..), Constructor(..), NoArguments(..), Field(..), Product(..), Rec(..), Sum(..), to)
import Data.List (List(..), (:), fromFoldable, null, singleton)
import Data.Symbol (class IsSymbol, SProxy(..), reflectSymbol)
import Type.Proxy (Proxy(..))

class FromDynamo a <= DynamoAttribute a where
    dynamoType :: (Proxy a) -> String


instance fromDynamoString :: FromDynamo String where
    dynamoRead = readString

instance dynamoAttrString :: DynamoAttribute String where
    dynamoType _ = "S"

instance fromDynamoNumber :: FromDynamo Number where
    dynamoRead = readJSON <=< readString

instance dynamoAttrNumber :: DynamoAttribute Number where
    dynamoType _ = "N"

instance fromDynamoInt :: FromDynamo Int where
    dynamoRead = readJSON <=< readString

instance dynamoAttrInt :: DynamoAttribute Int where
    dynamoType _ = "N"

readDynamoProp :: forall a. (DynamoAttribute a) => Foreign -> F a
readDynamoProp = dynamoRead <=< readProp (dynamoType (Proxy :: Proxy a))


class DynamoDecode a where
    decode :: Foreign -> F a

class DynamoDecodeArgs a where
    decodeArgs :: Int -> List Foreign -> F { result :: a
                                           , rest :: List Foreign
                                           , next :: Int
                                           }

class DynamoDecodeFields a where
  decodeFields :: Foreign -> F a

instance dynamoDecodeConstructor
  :: (IsSymbol name, DynamoDecodeArgs rep, GenericCountArgs rep)
  => DynamoDecode (Constructor name rep) where
  decode f = Constructor <$> readArguments f
    where
        numArgs = countArgs (Proxy :: Proxy rep)
        readArguments args =
            case numArgs of
                Left a -> pure a
                Right 1 -> do
                    { result, rest } <- decodeArgs 0 (singleton args)
                    unless (null rest) $
                      fail (ForeignError "Expected a single argument")
                    pure result
                Right n -> do
                    vals <- readArray args
                    { result, rest } <- decodeArgs 0 (fromFoldable vals)
                    unless (null rest) $
                      fail (ForeignError ("Expected " <> show n <> " constructor arguments"))
                    pure result

instance dynamoDecodeSum
  :: (DynamoDecode a, DynamoDecode b)
  => DynamoDecode (Sum a b) where
  decode f = Inl <$> decode f <|> Inr <$> decode f

instance dynamoDecodeArgsNoArguments :: DynamoDecodeArgs NoArguments where
  decodeArgs i Nil = pure { result: NoArguments, rest: Nil, next: i }
  decodeArgs _ _ = fail (ForeignError "Too many constructor arguments")

instance dynamoDecodeArgsArgument
  :: IsForeign a
  => DynamoDecodeArgs (Argument a) where
  decodeArgs i (x : xs) = do
    a <- mapExcept (lmap (map (ErrorAtIndex i))) (read x)
    pure { result: Argument a, rest: xs, next: i + 1 }
  decodeArgs _ _ = fail (ForeignError "Not enough constructor arguments")

instance dynamoDecodeArgsProduct
  :: (DynamoDecodeArgs a, DynamoDecodeArgs b)
  => DynamoDecodeArgs (Product a b) where
  decodeArgs i xs = do
    { result: resA, rest: xs1, next: i1 } <- decodeArgs i xs
    { result: resB, rest, next } <- decodeArgs i1 xs1
    pure { result: Product resA resB, rest, next }

instance dynamoDecodeArgsRec
  :: DynamoDecodeFields fields
  => DynamoDecodeArgs (Rec fields) where
  decodeArgs i (x : xs) = do
    fields <- mapExcept (lmap (map (ErrorAtIndex i))) (decodeFields x)
    pure { result: Rec fields, rest: xs, next: i + 1 }
  decodeArgs _ _ = fail (ForeignError "Not enough constructor arguments")


instance dynamoDecodeFieldsField
  :: (IsSymbol name, DynamoAttribute a)
  => DynamoDecodeFields (Field name a) where
  decodeFields x = do
    let name = reflectSymbol (SProxy :: SProxy name)
    -- If `name` field doesn't exist, then `y` will be `undefined`.
    Field <$> (readDynamoProp =<< readProp name x)

instance dynamoDecodeFieldsProduct
  :: (DynamoDecodeFields a, DynamoDecodeFields b)
  => DynamoDecodeFields (Product a b) where
  decodeFields x = Product <$> decodeFields x <*> decodeFields x



dynamoReadGeneric :: forall a rep. (Generic a rep, DynamoDecode rep) => Foreign -> F a
dynamoReadGeneric = map to <<< decode

class FromDynamo a where
  dynamoRead :: Foreign -> F a
