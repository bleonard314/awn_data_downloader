library(RSelenium)
library(tidyverse)
library(furrr)

# if user forgets to stop server it will be garbage collected.
# rm(rD)
# gc()
# system("taskkill /im java.exe /f", intern = FALSE, ignore.stdout = FALSE)

awn_data_downloader <- function(station, start, end, username, password, path, default_fname, wait, max_attempts) {
  
  # Set start and end dates separated by a week
  start_dates <- seq.Date(as.Date(start), as.Date(end) - 1, 7)
  end_dates <- seq.Date(as.Date(start)+7, as.Date(end), 7)
  
  # Create Firefox profile for downloading
  fprof <-
    makeFirefoxProfile(
      list(
        browser.download.dir = path %>% str_replace_all("\\\\", "\\\\\\\\"),
        browser.download.folderList = 2L,
        browser.download.manager.showWhenStarting = FALSE,
        browser.helperApps.alwaysAsk.force = FALSE,
        browser.download.manager.showWhenStarting = FALSE,
        browser.helperApps.neverAsk.saveToDisk = "text/csv"
      )
    )
  
  # Start browser and navigate to webpage
  rd <- rsDriver(
    browser = "firefox",
    chromever = NULL,
    extraCapabilities = fprof
  )
  
  remDr <- rd[["client"]]
  
  # Log in
  remDr$navigate("http://weather.wsu.edu/?p=90550&desktop")
  remDr$findElement("id", "user")$sendKeysToElement(list(username))
  remDr$findElement("name", "pass")$sendKeysToElement(list(password))
  remDr$findElement("xpath", "/html/body/div[1]/div/main/section/div/article/div[2]/form/div/div[2]/div/input")$clickElement()
  
  # Get to download page
  remDr$navigate("http://weather.wsu.edu/?p=92850&desktop")
  Sys.sleep(1)
  remDr$findElement("xpath", paste0("//option[. = '", station, "']"))$clickElement()
  
  get_weekly_data <- function(i) {
    
    start = start_dates[i]
    end = end_dates[i]
    
    # Apply filters and download each date range
    remDr$findElement("id", "datepickerstart")$clearElement()
    remDr$findElement("id", "datepickerstart")$sendKeysToElement(list(start))
    
    remDr$findElement("id", "datepickerend")$clearElement()
    remDr$findElement("id", "datepickerend")$sendKeysToElement(list(end))
    Sys.sleep(0.5)
    remDr$findElement("xpath", "//input[@value='Submit']")$clickElement()
    
    Sys.sleep(wait)
    
    if (default_fname == "AWN_Data_View_15.csv") {
      remDr$findElement("id", "downloadbutton")$clickElement()
      try(remDr$acceptAlert(), silent = TRUE)
    } else {
      webElem <- NULL
      attempt <- 1
      while (is.null(webElem)) {
        try(webElem <- remDr$findElement("link text", "CSV"), silent = TRUE)
        if (is.null(webElem)) {
          
          if (attempt > max_attempts) {
            warning(paste0("Download attempt #", attempt, " failed. Skipping index #", i, "..."))
            return()
          } else {
            warning(paste0("Download attempt #", attempt, " failed. Trying again..."))
            attempt <- attempt + 1
            Sys.sleep(0.5)
          }
          
        }
      }
      webElem$clickElement()
    }
    
    Sys.sleep(0.5) # Set to longer if on a slow connection
    
    fname <- file.path(path, default_fname)
    if (default_fname == "AWN_Data_View_15.csv") {
      dat <- read_csv(fname, skip = 2) # Read saved file
    } else {
      dat <- read_csv(fname, show_col_types = FALSE) # Read saved file
    }
    
    file.remove(fname)
    
    return(dat)
    
  }
  
  df <- seq_along(end_dates) %>%
    purrr::map(~get_weekly_data(.x), .progress = TRUE) %>%
    purrr::list_rbind()
  
  remDr$close()
  rd$server$stop()
  
  return(df)
  
}

get_awn_data <- function(station = "Tumwater", start = "2020-10-21", end = "2021-11-24", username = "BLeonard", password = "pQGt3F14", path = tempdir(), download_table = TRUE, wait = 2, max_attempts = 5) {
  
  if (download_table) {
    default_fname = "Fifteen Minute Data AgWeatherNet at Washington State University.csv"
  } else {
    default_fname = "AWN_Data_View_15.csv"
  }
  
  # Download Data
  d <- awn_data_downloader(station, start, end, username, password, path, default_fname, wait, max_attempts)
  
  format = "%Y-%m-%d %H:%M:%S" # Set time format
  
  awn_data <- d  %>%
    distinct() %>% # Get distinct records
    mutate(
      TimePDT = replace_na(TimePDT, 00:00:00),
      timestamp = as.POSIXct(paste0(Date, TimePDT), tz = "etc/GMT+7", format = format),
      timestamp = with_tz(timestamp, tzone = "etc/GMT+8") # not necessary since it will left_join into the right tzone
    ) %>%
    arrange(timestamp)
  
  return(awn_data)
  
}

# Example #1: Bulk Execution
# puyallup_awn_data <- get_awn_data(station = "Puyallup", start = "2003-01-01", end = "2023-06-30")

# Example #2: Annual Intermediates
# 2003:2023 %>%
  # map(~get_awn_data(station = "Puyallup", start = paste0(.x, "-01-01"), end = paste0(.x, "-12-31")) %>% write_rds(paste0("puyallup_awn_data_", .x, ".rds")))
#
# Read and add QC columns
# puyallup_awn_data <- list.files(pattern = "\\.rds$") %>%
#   purrr::map(~read_rds(.x)) %>%
#   purrr::list_rbind() %>%
#   distinct() %>%
#   arrange(timestamp) %>%
#   mutate(
#     `Previous Record Duplicate` = timestamp == lag(timestamp),
#     `Previous Record Missing` = timestamp != lag(timestamp) + lubridate::minutes(15)
#   )
#
# Save
# puyallup_awn_data %>% write_csv("puyallup_awn_data_2003-01-01_2023-07-19.csv")
# 
# puyallup_awn_data %>% write_rds("puyallup_awn_data_2003-01-01_2023-07-19.rds")

