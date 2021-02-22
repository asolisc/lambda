## ----setup, include=FALSE--------------------------------
knitr::opts_chunk$set(
  echo = T,
  warning = F)

library(tidyverse)
library(lubridate)
library(glue)
library(skimr)
library(highcharter)


## ---- eval=FALSE-----------------------------------------
## # NOT RUN
## # install.packages(c("tidyverse", "lubridate", "glue", "skimr", "highcharter"))


## --------------------------------------------------------
constituents <- read_csv("data/constituents.csv", col_types = "dDcccccc") %>% 
  # Remove first column, which has the index
  select(-1)

constituents


## --------------------------------------------------------
constituents %>% 
  skimr::skim()


## --------------------------------------------------------
constituents %>% distinct(action)


## --------------------------------------------------------
constituents %>% 
  filter(!is.na(contraticker) | !is.na(contraname)) %>% 
  distinct(action)


## --------------------------------------------------------
constituents %>% 
  filter(!is.na(contraticker) | !is.na(contraname)) 


## --------------------------------------------------------
constituents %>% distinct(ticker, name) %>% count(ticker, name, name = "count")


## --------------------------------------------------------
constituents %>% 
  distinct(ticker, name) %>% 
  count(ticker, name, name = "count") %>% 
  filter(count > 1)


## --------------------------------------------------------
add_ts_signature <- function(df, .date_col) {
  df %>% 
    # Create time series signature
    mutate(
      date_year        = year({{.date_col}}),
      date_half        = semester({{.date_col}}, with_year = FALSE),
      date_quarter     = quarter({{.date_col}}),
      date_month       = month({{.date_col}}),
      date_month_label = month({{.date_col}}, label = TRUE),
      date_day         = day({{.date_col}}),
      date_wday        = wday({{.date_col}}, week_start = 1),
      date_wday_label  = wday({{.date_col}}, label = TRUE),
      date_qday        = qday({{.date_col}}),
      date_yday        = yday({{.date_col}}),
      date_qid         = str_c(date_quarter,"Q", str_sub(date_year, 3L)) %>% as_factor()
    )
}


## --------------------------------------------------------
constituents_ts <- constituents %>% 
  add_ts_signature(.date_col = date)


## --------------------------------------------------------
constituents_ts %>% count(action, sort = T)


## --------------------------------------------------------
constituents_ts %>% 
  filter(
    action == "current"
  ) %>% 
  distinct(date)


## --------------------------------------------------------
constituents_ts %>% 
  filter(
    date == ymd("1957-03-04")
  )


## --------------------------------------------------------
constituents %>% 
  filter(date == ymd("2020-12-31"))


## --------------------------------------------------------
constituents %>% 
  filter(date == ymd("2020-12-31")) %>% 
  count(action)


## --------------------------------------------------------
constituents_wide <- constituents_ts %>% 
  pivot_wider(
    names_from = action,
    values_from = name
  ) %>% 
  relocate(historical, current, added, removed, .after = ticker)

constituents_wide


## --------------------------------------------------------
constituents_wide %>% 
  filter(!is.na(historical)) %>%
  distinct(date_month, date_quarter)


## --------------------------------------------------------
constituents_wide %>% 
  select(date_year, date_qid, historical) %>% 
  filter(date_year > 2008, !is.na(historical)) %>% 
  group_by(date_year, date_qid) %>%  
  nest() %>% 
  mutate(constituent_count = map_int(data, nrow)) %>% 
  select(-data) %>% 
  print(n = 45)


## --------------------------------------------------------
adds_drops <- constituents_wide %>% 
  filter(!is.na(added) | !is.na(removed) , date_qid %in% c("4Q20")) %>% 
  select(date, ticker, added, removed, date_qid)

adds_drops %>% 
  filter(!is.na(added)) %>% 
  select(-removed)


## --------------------------------------------------------
adds_drops %>% 
  filter(!is.na(removed)) %>% 
  select(-added)


## --------------------------------------------------------
# Extract the adds from 4Q20 to 3Q20
adds_3Q <- adds_drops %>% 
  filter(!is.na(removed)) %>% 
  pull(removed)

adds_3Q


## --------------------------------------------------------
# Extract the drops from 4Q20 to 3Q20
drops_3Q <- adds_drops %>% 
  filter(!is.na(added)) %>% 
  pull(added)

drops_3Q


## --------------------------------------------------------
# Get all the constituents for 4Q20
constituents_4Q20 <- constituents_wide %>% 
  filter(date_qid == "4Q20", !is.na(historical)) %>% 
  pull(historical) %>% sort()

# Get all the constituents for 3Q20
constituents_3Q20 <- constituents_wide %>% 
  filter(date_qid == "3Q20", !is.na(historical)) %>% 
  pull(historical) %>% sort()

# include the adds from 4Q20
calculated_3Q20 <- c(constituents_4Q20, adds_3Q)

# remove the drops from 4Q20
calculated_3Q20 <- calculated_3Q20[!calculated_3Q20 %in% drops_3Q] %>% sort()

# Check if both results are equal
all.equal(constituents_3Q20, calculated_3Q20)


## --------------------------------------------------------
# Get the required dates from the closes.csv file
required_dates <- read_csv(file = "data/closes.csv") %>% 
  # Focus on the date only
  select(date) %>% 
  
  # Focus on 2010 or later
  filter(year(date) > 2008)


## --------------------------------------------------------
constituents_nested <- constituents_wide %>% 
  # Focus on just the historicals and 2010 or later
  filter(date_year > 2008, !is.na(historical)) %>% 
  
  # Just take the date, ticker, and historical columns
  select(date, ticker, historical) %>% 
  
  # Nest the data
  nest(constituents = c(ticker, historical)) %>% 
  relocate(constituents, .after = 1) %>% 
  arrange(date) %>% 
  
  # Uncomment in case we want to get the number of constituents per date
  # mutate(constituent_count = map_int(constituents, nrow)) %>% 
  identity()

constituents_nested


## --------------------------------------------------------
constituents_nested <- required_dates %>% 
  left_join(constituents_nested, by = "date") %>% 
  fill(contains("constituent"), .direction = "up")

constituents_nested


## --------------------------------------------------------
# Use read_csv() function to read the close data
close_tbl <- read_csv("data/closes.csv") %>% 
  select(-1)

close_tbl


## --------------------------------------------------------
# This functions needs a data frame that has a date column and the rest of the columns must have the price of the securities.
add_price_roc <- function(df, .date_col, .ndays) {
  
  df %>% 
    mutate(
      across(
        .cols = -{{.date_col}},
        .fns = ~ ((.x / lag(.x, n = .ndays)) - 1) * 100,
        .names = "ROC_{.col}"
      )
    ) %>% 
    # keep rows where at least one of the ROC columns is not NA
    filter(
      if_any(contains("ROC"), ~ !is.na(.x))
    )
}


## --------------------------------------------------------
roc_close <- close_tbl %>% 
  # Add 200-day ROC
  add_price_roc(.date_col = date, 
                .ndays = 200) %>%
  relocate(date, contains("ROC")) %>% 
  
  # Add the returns
  mutate(
    across(
      .cols = -c(date, contains("ROC")),
      .fns = ~ (.x / lag(.x)) - 1,
      .names = "return_{.col}"
    )
  )

roc_close


## --------------------------------------------------------
roc_close <- roc_close %>% 
  filter(year(date) > 2008)


## --------------------------------------------------------
# Build a dataframe with ROC values
roc_long <- roc_close %>% 
  select(date, contains("ROC")) %>% 
  pivot_longer(
    cols = contains("ROC"),
    names_to = "ticker",
    names_pattern = "_(.*)",
    values_to = "roc"
  ) %>% 
  add_column(data_type = "ROC")

roc_long


## --------------------------------------------------------
# Build a dataframe with price values
close_long <- roc_close %>% 
  select(-contains("ROC")) %>% 
  select(-contains("return")) %>% 
  pivot_longer(
    cols = -date,
    names_to = "ticker",
    values_to = "price"
  ) %>% 
  add_column(data_type = "close")

close_long


## --------------------------------------------------------
# Build a dataframe with ROC values
returns_long <- roc_close %>% 
  select(date, contains("return")) %>% 
  pivot_longer(
    cols = contains("return"),
    names_to = "ticker",
    names_pattern = "_(.*)",
    values_to = "return"
  ) %>% 
  add_column(data_type = "return")

returns_long


## --------------------------------------------------------
roc_nested <- roc_long %>% 
  nest(rocs = c(ticker, roc, data_type))
  
roc_nested


## --------------------------------------------------------
close_nested <- close_long %>% 
  nest(prices = c(ticker, price, data_type))

close_nested


## --------------------------------------------------------
returns_nested <- returns_long %>% 
  nest(returns = c(ticker, return, data_type))

returns_nested


## --------------------------------------------------------
consolidated_nested <- constituents_nested %>% 
  left_join(close_nested, by = "date") %>% 
  left_join(roc_nested, by = "date") %>% 
  left_join(returns_nested, by = "date") %>% 
  mutate(row_id = row_number()) %>% 
  relocate(row_id, everything())

consolidated_nested


## --------------------------------------------------------
start_date <- ymd("2009-12-31")
end_date <- ymd("2020-01-02")


## --------------------------------------------------------
consolidated_nested <- consolidated_nested %>% 
  filter(date %>% between(start_date - 10, end_date + 10)
  )


## --------------------------------------------------------
extract_constituents <- function(df, extraction_date) {
  df %>% 
    filter(date == extraction_date) %>% 
    select(row_id, date, constituents) %>% 
    unnest(cols = constituents) %>% 
    pull(ticker)
}


## --------------------------------------------------------
extract_constituents(consolidated_nested, ymd("2019-12-31")) %>% 
  head()


## --------------------------------------------------------
show_top_roc <- function(df, extraction_date, .top_n = 10L) {
  if (!is.numeric(.top_n)) {
    stop("You must provide an integer for the top 'n' constituents.")
  }
  
  # Get the ID for the previous close
  previous_row <- df %>% 
    filter(date == extraction_date) %>% 
    pull(row_id) - 1
  
  # Proceed to get the top n tickers
  df %>% 
    filter(row_id == previous_row) %>% 
    select(row_id, date, rocs) %>% 
    unnest(cols = rocs) %>% 
    
    # Filter to focus on the S&P 500 constituents. Use extract_constituents function
    filter(ticker %in% extract_constituents(df, extraction_date)) %>% 
    
    # Get the top n, as ordered by ROC
    slice_max(order_by = roc, n = .top_n)
  
}


## --------------------------------------------------------
show_top_roc(consolidated_nested, ymd("2019-12-31"), .top_n = 10)


## --------------------------------------------------------
extract_top_roc <- function(df, extraction_date, .top_n = 10L) {
  
  show_top_roc(df, extraction_date, .top_n) %>% 
    pull(ticker)
}


## --------------------------------------------------------
extract_top_roc(consolidated_nested, ymd("2019-12-31"), .top_n = 10)


## --------------------------------------------------------
calculate_weights <- function(df, .date, .top_n = 10L) {
  chosen_tickers <- extract_top_roc(df, .date, .top_n)
  
  weights_tbl <- tibble(
    date = .date,
    tickers = extract_constituents(df, .date),
    weights = if_else(
      tickers %in% chosen_tickers, 
      1 / length(chosen_tickers), # equally weighted portfolio
      0                           # if ticker is not chose, assign 0 weight
    )
  ) %>% 
    # Focus on stocks that are chosen
    filter(weights > 0) %>% 
    
    arrange(tickers)
  
  
  return(weights_tbl)
}


## --------------------------------------------------------
calculate_weights_wide <- function(df, .date, .top_n = 10L) {
  
  weights_tbl <- calculate_weights(df, .date, .top_n = .top_n)
  
  tickers_tbl <- weights_tbl %>%
    add_column("ticker_id" = paste0("ticker_", 1:.top_n)) %>% 
    pivot_wider(
      id_cols = date,
      names_from = ticker_id,
      values_from = tickers
    )
  
  weights_tbl <- weights_tbl %>%
    add_column("weight_id" = paste0("weight_", 1:.top_n)) %>% 
    pivot_wider(
      id_cols = date,
      names_from = weight_id,
      values_from = weights
    ) 

  return(left_join(tickers_tbl, weights_tbl, by = "date"))
    
}


## --------------------------------------------------------
calculate_weights(consolidated_nested, ymd("2019-12-31"), .top_n = 10)


## --------------------------------------------------------
calculate_weights_wide(consolidated_nested, ymd("2019-12-31"), .top_n = 10)


## --------------------------------------------------------
# Extract the dates needed for the analysis
analysis_dates <- consolidated_nested %>% 
  select(date) %>% 
  filter(
    date %>% between(ymd("2009-12-31"), ymd("2020-01-02"))
    ) %>% pull(date)

analysis_dates %>% head()


## ---- cache=T--------------------------------------------
# WARNING: This takes ~ 3-4 min to to run
strategy_weights <- analysis_dates %>% 
  map_dfr(.f = ~ calculate_weights_wide(consolidated_nested, .x, .top_n = 10))

strategy_weights

strategy_weights_long <- analysis_dates %>% 
  map_dfr(.f = ~ calculate_weights(consolidated_nested, .x, .top_n = 10))


## --------------------------------------------------------
strategy_weights %>% 
  write_excel_csv("output/weights.csv")


## --------------------------------------------------------
weights_n_returns <- strategy_weights_long %>% rename(ticker = tickers) %>% 
  left_join(returns_long, by = c("date", "ticker")) %>% 
  select(-data_type)

weights_n_returns


## --------------------------------------------------------
strategy_returns <- weights_n_returns %>% 
  
  # calculate the weighted return
  mutate(weighted_return = weights * return) %>% 
  group_by(date) %>% 
  
  # sum the returns for each date
  summarise(strategy_return = sum(weighted_return)) %>% 
  
  # get a column with the NAV multiplier.
  mutate(multiplier = if_else(date == analysis_dates[[1]], 1, strategy_return + 1),
         cumulative_multiplier = cumprod(multiplier))

strategy_returns


## --------------------------------------------------------
starting_capital <- 100

strategy_nav <- strategy_returns %>% 
  mutate(nav = starting_capital * cumulative_multiplier) %>% 
  select(date, strategy_return, nav)

strategy_nav


## --------------------------------------------------------
strategy_nav %>% 
  write_excel_csv("output/nav.csv")


## --------------------------------------------------------
strategy_nav %>% 
  hchart("line",
         hcaes(date, nav),
         color = "#002C54") %>% 
         # color = "#258039") %>% 
    hc_title(
    text = "200-day ROC Strategy's Net Asset Value"
    ) %>% 
  hc_add_theme(hc_theme_538())

