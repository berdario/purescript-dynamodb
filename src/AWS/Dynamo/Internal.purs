module AWS.Dynamo.Internal
    ( Tablename
    , HashKey
    , SortKey
    , Table
    , UntypedTable
    , tableSpec
    , Order(..)
    , AttrVal
    , hashAttr
    , sortAttr
    , KeyExpr(..)
    , SubKeyExpr(..)
    , BinaryOp(..)
    , compileCondition
    , attrValsToForeign
    , renderErrors
    )
where

import Prelude
import Control.Monad.Except (runExcept, withExcept)
import Data.Either (Either)
import Data.Foreign (F, MultipleErrors, renderForeignError, Foreign, writeObject)
import Data.Foreign.Class (class AsForeign, writeProp, write)
import Data.List.NonEmpty (toUnfoldable)
import Data.Maybe (maybe, Maybe)
import Data.String (joinWith)


foreignParseToEither :: forall a. F a -> Either String a
foreignParseToEither = runExcept <<< (withExcept renderErrors)

renderErrors :: MultipleErrors -> String
renderErrors = joinWith "\n" <<< toUnfoldable <<< map renderForeignError


newtype Tablename a = Tablename String
newtype HashKey a hk = HashKey String
newtype SortKey a sk = SortKey String

type Table a hk sk =
    { name :: Tablename a
    , hashkey :: HashKey a hk
    , sortkey :: SortKey a sk
    }

type UntypedTable =
    { name :: String
    , hashkey :: String
    , sortkey :: String
    }

tableSpec :: forall a hk sk. UntypedTable -> Table a hk sk
tableSpec {name, hashkey, sortkey} =
    { name : Tablename name
    , hashkey : HashKey hashkey
    , sortkey : SortKey sortkey
    }

instance tablenameAsForeign :: AsForeign (Tablename a) where
    write (Tablename s) = write s


data Order = Ascending | Descending

instance orderAsForeign :: AsForeign Order where
    write Ascending = write true
    write Descending = write false

data AttrVal a typ = AttrVal
    { name :: String
    , value :: typ
    }

hashAttr :: forall a typ. (HashKey a typ) -> typ -> AttrVal a typ
hashAttr (HashKey name) value = AttrVal
    { name
    , value
    }

sortAttr :: forall a typ. (SortKey a typ) -> typ -> AttrVal a typ
sortAttr (SortKey name) value = AttrVal
    { name
    , value
    }

data KeyExpr a hk sk = KeyEq (HashKey a hk) (AttrVal a hk) (Maybe (SubKeyExpr a sk))

data BinaryOp = CEQ | CLT | CLE | CBT | CBE

showOp :: BinaryOp -> String
showOp CEQ = "="
showOp CLT = "<"
showOp CLE = "<="
showOp CBT = ">"
showOp CBE = ">="

data SubKeyExpr a sk
    = BinCond BinaryOp (SortKey a sk) (AttrVal a sk)
    | Between (SortKey a sk) (AttrVal a sk) (AttrVal a sk)

compileCondition :: forall a hk sk. KeyExpr a hk sk -> String
compileCondition (KeyEq (HashKey hkey) (AttrVal {name}) subCond)
    = hkey <> "=:" <> name <> (maybe "" doCompile subCond)
        where
            doCompile cond = " AND " <> compileSubCondition cond

compileSubCondition :: forall a sk. SubKeyExpr a sk -> String
compileSubCondition (Between (SortKey skey) (AttrVal lower) (AttrVal higher))
    = skey <> " BETWEEN :" <> lower.name <> " AND :" <> higher.name
compileSubCondition (BinCond binaryOp (SortKey skey) (AttrVal {name}))
    = skey <> showOp binaryOp <> ":" <> name

attrValsToForeign :: forall a hk sk
                   . (AsForeign hk, AsForeign sk)
                  => { keyAttr:: AttrVal a hk
                     , sortAttrs :: Array (AttrVal a sk) } -> Foreign
attrValsToForeign {keyAttr: AttrVal key, sortAttrs}
    = writeObject $ [ writeProp (":" <> key.name) key.value] <> sortProps
        where
            sortProps = map (\(AttrVal {name, value}) -> writeProp (":" <> name) value) sortAttrs
