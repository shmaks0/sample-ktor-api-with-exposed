# sample-ktor-api-with-exposed
Sample Ktor API with H2 and exposed for access

1) Create account:  
POST http://0.0.0.0:8080/api/v1/accounts  
{"name": <name (string)>,"clientId":<client id (string)>, "currencyCode":<USD|EUR|GBP (string)>,"balance":<starting balance (decimal)>}
  
2) Perform money transfer:  
POST http://0.0.0.0:8080/api/v1/transfers  
{"from": <source_acc_number (long)>,"to":<to_acc_number (long) >,"currencyCode":<USD|EUR|GBP>,"amount":<decimal>}
