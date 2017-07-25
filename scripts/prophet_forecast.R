# Set current working directory
setwd("~/Documents/kaggle/web-traffic-time-series-forecasting")
# setwd("~/web-traffic-time-series-forecasting")

# Source required scripts
source("./scripts/load_data.R")

# Load required libraries
library(doParallel)
library(prophet)

# Function to remove outliers
remove_outliers <- function(x, na.rm = TRUE, ...) {
  qnt <- quantile(x, probs=c(.25, .75), na.rm = na.rm, ...)
  H <- 1.5 * IQR(x, na.rm = na.rm)
  y <- x
  y[x < (qnt[1] - H)] <- NA
  y[x > (qnt[2] + H)] <- NA
  y
}

prophet_forecast <- function(row) {
  tryCatch({
    series.name <- row[1]
    series <- as.data.frame(t(row[2:ncol(row)]))
    colnames(series) <- c("y")
    series$ds <- as.Date(rownames(series), format = "X%Y.%m.%d")
    rownames(series) <- NULL
    # Remove outliers
    series$y <- remove_outliers(series$y)
    # Prophet
    m <- prophet(series, weekly.seasonality = TRUE, yearly.seasonality = FALSE)
    future <- make_future_dataframe(m, periods = 60, include_history = FALSE)
    forecast <- predict(m, future)
    forecast$id <- paste0(series.name, "_", as.character(forecast$ds))
    forecast[, c("id", "yhat")]
  }, error = function(err) {
    series.name <- row[1]
    forecast <- data.frame(
      id=paste0(series.name, "_", seq.Date(as.Date("2017-01-01"), as.Date("2017-03-01"), by = "day")),
      yhat=0, stringsAsFactors = FALSE
    )
    forecast
  })
}


# Perform forecast
no_cores <- detectCores() - 1
registerDoParallel(cores=no_cores)
cl <- makeCluster(no_cores)
result <- foreach(i = 1:nrow(train), .combine = rbind) %dopar% {
  prophet_forecast(train[i,])
}
stopCluster(cl)
colnames(result) <- c("Page", "Visits")
write.csv(result, file = "./output/prophet_nys.csv", row.names = FALSE)
