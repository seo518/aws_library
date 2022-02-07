library(RCurl)
library(httr)
library(jsonlite)

file <- read.as.raw("C:/Users/miguel.ajon/Documents/test.txt")

p = base64_enc(file)

x <- GET("http://worldclockapi.com/api/json/est/now")
{id:1000}



# real function
api_url = "https://6wq1ewua1h.execute-api.ca-central-1.amazonaws.com/default/dev-Router?functionName=list_s3_files"


# dummypasser - router
api_url = "https://6wq1ewua1h.execute-api.ca-central-1.amazonaws.com/default/dev-Router?functionName=dummyPasser&invoke=RequestResponse"
apikey = "4C0ylRZIRh3Z8DmRooZsM1bHAzex7V2o3lL7d2TH"

## dummypasser direct
api_url = "https://dtgzjfcr47.execute-api.ca-central-1.amazonaws.com/default/dev-dummyPasser"
apikey = "f5Pwq763McqCMM7CoNob4Kg4x20PMJ79bSGCb2U3"

# dummypasser - router
api_url = "https://6wq1ewua1h.execute-api.ca-central-1.amazonaws.com/default/dev-Router?functionName=download_s3_files&invoke=RequestResponse"
apikey = "4C0ylRZIRh3Z8DmRooZsM1bHAzex7V2o3lL7d2TH"

payload_test <- RJSONIO::toJSON(list(bucket = "apnreferencefields"))
x <- POST(api_url,config = add_headers("x-api-key" = apikey ), 
         body = payload_test, 
         encode = "json",
         verbose())
x$status_code
x$request
print(content(x, "text"))


x$content
x$all_headers





# Inputs:
# required params:
#   to = str/list
#   subject = str
#   filename = str = "NA"
#   key = str
# post 
#   attachment = Base64 string
#   username = str
#   password = str
#   {
#        "attachment": "BASE64 STRING"
#        "username": "YOUR USERNAME"
#        "password": "YOUR PASSWORD"
#   }
library(httr)
library(RJSONIO)
library(base64enc)
f <- "C:/Users/miguel.ajon/Documents/AWS/20191001_FW_Tags_2019_PCB_M19_009_PCF_Digital_Q3_PCMC_Presidents_Choice_DV360_MOAT_DISPLAY.xls"
a <- file(f, "rb")
base64_string<- base64enc::base64encode(a)

payload_test <- toJSON(list(attachment=base64_string, username = "xaxis.analyticstor@gmail.com", password = "xaxis.com"))

url_api <- URLencode("https://1whnajy22a.execute-api.ca-central-1.amazonaws.com/default/sendEmail?to=ajon.jm@gmail.com&subject=test&key=xaxisemail&filename=NA")

post_request <- POST(url = url_api, body = payload_test, encode = "json", config = add_headers("x-api-key" = "WJpjmPIx2GaDknep0bM9u1TgOJEx3gSlaGK08kJ9"), verbose())
post_request$status_code
content(post_request, "text")

"https://1whnajy22a.execute-api.ca-central-1.amazonaws.com/default/sendEmail?to=shweta.anjan@groupm.com&subject=20191001_FW_Tags_2019_PCB_M19_009_PCF_Digital_Q3_PCMC_Presidents_Choice_DV360_MOAT_DISPLAY&filename=20191001_FW_Tags_2019_PCB_M19_009_PCF_Digital_Q3_PCMC_Presidents_Choice_DV360_MOAT_DISPLAY.csv&key=xaxisemail"

config = add_headers("x-api-key" = "WxPpOaIb5ZEitZEJjO0j8UpALgUUyiY4xizfs4o4")



URLencode("xaxis.analyticstor@gmail.com")
x <- GET("https://www.googleapis.com/gmail/v1/users/xaxis.analyticstor@gmail.com/profile?key=AIzaSyCvccU8-wHp2BFg2U6nESKXpltCecjQSDU")
x$status_code


x <- scan("C:/Users/miguel.ajon/Documents/aws_library/mimetextr.txt", what = "character")
a <- unlist(strsplit(x,split = "\\\n"))
a


