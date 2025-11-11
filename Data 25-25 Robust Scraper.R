library(worldfootballR)
library(dplyr)
library(stringi)
library(RSelenium)
library(netstat)
library(xml2)
library(purrr)
library(binman)
library(chromote)
library(rvest)
library(selenider)
library(magrittr)
library(tidyverse)
library(glue)
library(progress)
library(janitor)

# Explicitly load chromote to ensure options function is available
library(chromote)


#' Get team match log stats (Selenium Version)
#'
#' Scrapes detailed match statistics (e.g., passing, shooting) for a team from fbref.com
#' using a headless browser (via selenider/chromote) to bypass Cloudflare protection.
#'
#' @param team_urls Vector of team URLs (e.g., https://fbref.com/en/squads/d48ad4ff/Napoli-Stats).
#' @param stat_type The type of statistic required (e.g., "passing", "shooting").
#' @param time_pause Base wait time (in seconds) between page loads to throttle requests.
#' @param session Optional active selenider session. If NULL, creates a new visible one.
#' @return Dataframe of joined match logs (combining "For" and "Against" stats).
#' @export
fb_team_match_log_stats_selenium <- function(team_urls, stat_type, time_pause = 10, session = NULL) {

  # --- 1. Internal Helper Functions ---
  smart_sleep <- function(base_wait) {
    # Adds random jitter (0.2 to 2.5s) to the base wait time to appear more human
    Sys.sleep(base_wait + runif(1, 0.2, 2.5))
  }

  apply_stealth <- function(sess) {
    # Critical CDP commands to modify navigator properties, hiding automation status from Cloudflare
    try({
      sess$driver$Network$setUserAgent(userAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
      sess$driver$Page$addScriptToEvaluateOnNewDocument(
         source = "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
      )
    }, silent = TRUE)
  }

  clean_table_names_int <- function(df) {
    # Uses janitor::clean_names for robust, standardized column naming
    if(requireNamespace("janitor", quietly = TRUE)) {
      janitor::clean_names(df, case = "parsed")
    } else {
      # Basic fallback if janitor isn't installed
      colnames(df) <- gsub(" ", "_", tolower(colnames(df)))
      df
    }
  }

  # --- 2. Session Management ---
  # If no session passed, create a NEW visible one and apply stealth
  if (is.null(session)) {
    message("🚀 Initializing new browser session...")
    session <- selenider_session(
      session = "chromote",
      options = chromote_options(headless = FALSE)
    )
    apply_stealth(session)
    # Ensure session closes when function finishes (on success or error)
    on.exit(try(session$close(), silent = TRUE), add = TRUE)
  }

  pb <- progress::progress_bar$new(
    format = "  Scraping [:bar] :percent in :elapsed",
    total = length(team_urls), clear = FALSE, width = 60
  )

  # --- 3. Main Scraping Loop ---
  scrape_single_team <- function(team_url) {
    pb$tick()
    # Initial wait before starting a new team
    smart_sleep(time_pause)

    tryCatch({
      # A. Navigate to Team Page
      selenider::open_url(team_url)

      # Wait for load. If it's the very first URL, wait extra long for potential auto-captcha solve
      smart_sleep(ifelse(team_url == team_urls[1], time_pause + 5, time_pause))

      team_page_source <- session %>% rvest::read_html()

      # B. Find Stat specific URL
      # Looks for the internal match logs link using the stat_type parameter
      xpath_selector <- glue::glue("//a[contains(@href, '/matchlogs/') and contains(@href, '/{stat_type}/')]")
      stat_links_nodes <- team_page_source %>% rvest::html_nodes(xpath = xpath_selector)
      stat_hrefs <- stat_links_nodes %>% rvest::html_attr("href") %>% unique()

      if(length(stat_hrefs) == 0) {
        warning(glue::glue("Stats link not found for {team_url}. Page might be blocked or invalid season."))
        return(data.frame())
      }

      full_stat_url <- paste0("https://fbref.com", stat_hrefs[1])

      # C. Navigate to Match Logs
      selenider::open_url(full_stat_url)
      smart_sleep(time_pause)

      # D. Extract Data
      stat_page_source <- session %>% rvest::read_html()
      # Target both standard and switcher table containers
      containers <- stat_page_source %>%
        rvest::html_nodes("#all_matchlogs .table_container, #switcher_matchlogs .table_container")

      if(length(containers) == 0) return(data.frame())

      # Helper to process For/Against tables
      process_table <- function(container_node, type_label) {
        df <- tryCatch(container_node %>% rvest::html_node("table") %>% rvest::html_table() %>% data.frame(), error = function(e) data.frame())
        if(nrow(df) == 0) return(df)

        # Remove "For xyz" or "Against xyz" super-headers from column names
        colnames(df)[grep(paste0("^", type_label), colnames(df), ignore.case = TRUE)] <- ""
        df <- clean_table_names_int(df)

        # Filter out header rows that snuck into the data
        if("date" %in% colnames(df)) df <- df %>% dplyr::filter(.data[["date"]] != "")
        df$for_against <- type_label

        # Try to find opponent column dynamically
        opp_idx <- grep("opponent", colnames(df), ignore.case = TRUE)[1]
        if(!is.na(opp_idx)) {
             # Re-scrape opponent names as html_table sometimes misses nested text in links
             opp_names <- container_node %>%
               rvest::html_nodes("tbody tr") %>%
               rvest::html_node(paste0("td:nth-child(", opp_idx, ")")) %>%
               rvest::html_text()
             # Ensure lengths match (sometimes spacers exist)
             if(length(opp_names) >= nrow(df)) {
                 df[[opp_idx]] <- opp_names[1:nrow(df)]
             }
        }
        return(df)
      }

      # Extract both tables if they exist (For [1] and Against [2])
      df_for <- if(length(containers) >= 1) process_table(containers[1], "For") else data.frame()
      df_against <- if(length(containers) >= 2) process_table(containers[2], "Against") else data.frame()

      # Combine and tag with the source URL
      final_df <- dplyr::bind_rows(df_for, df_against)
      if(nrow(final_df) > 0) {
        final_df$team_url <- team_url
      }

      return(final_df)

    }, error = function(e) {
      warning(glue::glue("Error on {team_url}: {conditionMessage(e)}"))
      return(data.frame())
    })
  }

  # Map over all URLs and combine results
  all_stats <- purrr::map_df(team_urls, scrape_single_team)
  return(all_stats)
}

#' Scrape match results and fixtures using Selenium to handle Cloudflare.
#'
#' @param country The country name (e.g., "ITA").
#' @param gender The gender (e.g., "M").
#' @param season_end_year The end year of the season (e.g., 2026).
#' @param tier The tier of the league (e.g., "1st").
#' @param non_dom_league_url Optional URL for a non-domestic league.
#' @param session Optional active selenider session.
#' @param time_pause Base wait time (in seconds) between page loads.
#' @return Dataframe of match results with standardized column names.
fb_match_results <- function(country, gender, season_end_year, tier = "1st", non_dom_league_url = NA, session = NULL, time_pause = 3) {

  # --- 1. Internal Helper Functions ---
  smart_sleep <- function(base_wait) {
    Sys.sleep(base_wait + runif(1, 0.2, 2.5))
  }

  apply_stealth <- function(sess) {
    try({
      sess$driver$Network$setUserAgent(userAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
      sess$driver$Page$addScriptToEvaluateOnNewDocument(
         source = "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
      )
    }, silent = TRUE)
  }

  # --- 2. Session Setup ---
  if (is.null(session)) {
    message("🚀 Initializing new browser session for match results...")
    session <- selenider::selenider_session(
      session = "chromote",
      options = chromote_options(headless = FALSE)
    )
    apply_stealth(session)
    on.exit(try(session$close(), silent = TRUE), add = TRUE)
  }

  # --- 3. URL Generation & Correction ---
  if (is.na(non_dom_league_url)) {
    # Uses worldfootballR's internal logic to get league URLs
    urls <- worldfootballR::fb_league_urls(country, gender, season_end_year, tier)
  } else {
    urls <- non_dom_league_url
  }

  # --- 4. Main Scraping Loop ---
  all_seasons_df <- purrr::map_dfr(urls, function(url) {
    # Ensure the URL points to the 'Scores-and-Fixtures' page
    if (!grepl("Scores-and-Fixtures|schedule", url, ignore.case = TRUE)) {
       url <- gsub("/([^/]+)-Stats$", "/schedule/\\1-Scores-and-Fixtures", url)
    }

    message(paste("Scraping match results from:", url))

    tryCatch({
      selenider::open_url(url)
      smart_sleep(time_pause)

      page_html <- rvest::read_html(session)
      # Find the main scores and fixtures table
      table_node <- page_html %>%
        rvest::html_node("table[id^='sched_all'], table[id*='Scores-and-Fixtures'], table.stats_table")

      if (length(table_node) == 0 || is.na(table_node)) {
         warning(paste("Could not find fixtures table for URL:", url))
         return(data.frame())
      }

      df <- table_node %>% rvest::html_table() %>% janitor::clean_names()

      if (nrow(df) > 0) {
        df$Competition_URL <- url
      }
      return(df)

    }, error = function(e) {
      warning(paste("Error scraping", url, ":", e$message))
      return(data.frame())
    })
  })

  # --- 5. Post-Processing & Renaming ---
  if (nrow(all_seasons_df) > 0) {
    all_seasons_df <- all_seasons_df %>%
      dplyr::filter(!is.na(date), date != "", tolower(date) != "date") %>%
      # Separate the Score column into HomeGoals and AwayGoals
      tidyr::separate(score, into = c("HomeGoals", "AwayGoals"), sep = "[–-]", remove = TRUE, convert = TRUE) %>%
      dplyr::rename(
        Wk = wk,
        Date = date,
        HomeTeam = home,
        AwayTeam = away,
        Home_xG = x_g,
        Away_xG = x_g_2
      ) %>%
      dplyr::mutate(
        Country = country,
        Gender = gender,
        Season_End_Year = season_end_year,
        Tier = tier
      )
  }

  return(all_seasons_df)
}

#' A robust function to run a scraping command with retries.
#'
#' @param expr The R expression to run (e.g., the fb_team_match_log_stats call)
#' @param max_retries The maximum number of times to try.
#' @param wait_seconds The number of seconds to wait between failures.
#' @return The result of the expression if successful.
#' @stop Stops the script if max_retries is exceeded.
safe_scrape_batch <- function(expr, max_retries = 5, wait_seconds = 30) {

  success <- FALSE
  retries <- 0
  result <- NULL

  while (!success && retries < max_retries) {
    tryCatch({

      # Try to execute the provided expression
      print(paste("Attempting scrape, try", retries + 1, "..."))
      result <- eval(expr)

      # If we get here, it worked!
      success <- TRUE
      print("Scrape successful.")

    }, error = function(e) {

      # This code runs ONLY if an error occurs
      retries <<- retries + 1 # Note: <<- modifies 'retries' outside this function
      print(paste("Error encountered:", e$message))

      if (retries < max_retries) {
        print(paste("Waiting", wait_seconds, "seconds before retrying..."))
        Sys.sleep(wait_seconds)
      } else {
        # If we've failed too many times, stop the whole script
        stop(paste("Max retries reached. Failed to scrape after", max_retries, "attempts."))
      }
    })
  }
  return(result)
}

# --- Main Execution Script ---

# 1. Define Team URLs
urls <- c(
  "https://fbref.com/en/squads/d48ad4ff/Napoli-Stats",
  "https://fbref.com/en/squads/d609edc0/Internazionale-Stats",
  "https://fbref.com/en/squads/dc56fe14/Milan-Stats",
  "https://fbref.com/en/squads/cf74a709/Roma-Stats",
  "https://fbref.com/en/squads/1d8099f8/Bologna-Stats",
  "https://fbref.com/en/squads/e0652b02/Juventus-Stats",
  "https://fbref.com/en/squads/28c9c3cd/Como-Stats",
  "https://fbref.com/en/squads/7213da33/Lazio-Stats",
  "https://fbref.com/en/squads/04eea015/Udinese-Stats",
  "https://fbref.com/en/squads/9aad3a77/Cremonese-Stats",
  "https://fbref.com/en/squads/922493f3/Atalanta-Stats",
  "https://fbref.com/en/squads/e2befd26/Sassuolo-Stats",
  "https://fbref.com/en/squads/105360fe/Torino-Stats",
  "https://fbref.com/en/squads/c4260e09/Cagliari-Stats",
  "https://fbref.com/en/squads/ffcbe334/Lecce-Stats",
  "https://fbref.com/en/squads/4cceedfc/Pisa-Stats",
  "https://fbref.com/en/squads/eab4234c/Parma-Stats",
  "https://fbref.com/en/squads/658bf2de/Genoa-Stats",
  "https://fbref.com/en/squads/0e72edf2/Hellas-Verona-Stats",
  "https://fbref.com/en/squads/421387cf/Fiorentina-Stats",
  "https://fbref.com/en/squads/0cdc4311/Augsburg-Stats",
  "https://fbref.com/en/squads/054efa67/Bayern-Munich-Stats",
  "https://fbref.com/en/squads/add600ae/Dortmund-Stats",
  "https://fbref.com/en/squads/f0ac8ee6/Eintracht-Frankfurt-Stats",
  "https://fbref.com/en/squads/a486e511/Freiburg-Stats",
  "https://fbref.com/en/squads/32f3ee20/Monchengladbach-Stats",
  "https://fbref.com/en/squads/26790c6a/Hamburger-SV-Stats",
  "https://fbref.com/en/squads/18d9d2a7/Heidenheim-Stats",
  "https://fbref.com/en/squads/033ea6b8/Hoffenheim-Stats",
  "https://fbref.com/en/squads/bc357bf7/Koln-Stats",
  "https://fbref.com/en/squads/c7a9f859/Bayer-Leverkusen-Stats",
  "https://fbref.com/en/squads/a224b06a/Mainz-05-Stats",
  "https://fbref.com/en/squads/acbb6a5b/RB-Leipzig-Stats",
  "https://fbref.com/en/squads/54864664/St-Pauli-Stats",
  "https://fbref.com/en/squads/598bc722/Stuttgart-Stats",
  "https://fbref.com/en/squads/7a41008f/Union-Berlin-Stats",
  "https://fbref.com/en/squads/62add3bf/Werder-Bremen-Stats",
  "https://fbref.com/en/squads/4eaa11d7/Wolfsburg-Stats",
  "https://fbref.com/en/squads/18bb7c10/Arsenal-Stats",
  "https://fbref.com/en/squads/b8fd03ef/Manchester-City-Stats",
  "https://fbref.com/en/squads/822bd0ba/Liverpool-Stats",
  "https://fbref.com/en/squads/8ef52968/Sunderland-Stats",
  "https://fbref.com/en/squads/4ba7cbea/Bournemouth-Stats",
  "https://fbref.com/en/squads/361ca564/Tottenham-Hotspur-Stats",
  "https://fbref.com/en/squads/cff3d9bb/Chelsea-Stats",
  "https://fbref.com/en/squads/19538871/Manchester-United-Stats",
  "https://fbref.com/en/squads/47c64c55/Crystal-Palace-Stats",
  "https://fbref.com/en/squads/d07537b9/Brighton-and-Hove-Albion-Stats",
  "https://fbref.com/en/squads/8602292d/Aston-Villa-Stats",
  "https://fbref.com/en/squads/cd051869/Brentford-Stats",
  "https://fbref.com/en/squads/b2b47a98/Newcastle-United-Stats",
  "https://fbref.com/en/squads/d3fd31cc/Everton-Stats",
  "https://fbref.com/en/squads/fd962109/Fulham-Stats",
  "https://fbref.com/en/squads/5bfb9659/Leeds-United-Stats",
  "https://fbref.com/en/squads/943e8050/Burnley-Stats",
  "https://fbref.com/en/squads/7c21e445/West-Ham-United-Stats",
  "https://fbref.com/en/squads/e4a775cb/Nottingham-Forest-Stats",
  "https://fbref.com/en/squads/8cec06e1/Wolverhampton-Wanderers-Stats",
  "https://fbref.com/en/squads/53a2f082/Real-Madrid-Stats",
  "https://fbref.com/en/squads/206d90db/Barcelona-Stats",
  "https://fbref.com/en/squads/2a8183b3/Villarreal-Stats",
  "https://fbref.com/en/squads/db3b9613/Atletico-Madrid-Stats",
  "https://fbref.com/en/squads/fc536746/Real-Betis-Stats",
  "https://fbref.com/en/squads/a8661628/Espanyol-Stats",
  "https://fbref.com/en/squads/7848bd64/Getafe-Stats",
  "https://fbref.com/en/squads/8d6fd021/Alaves-Stats",
  "https://fbref.com/en/squads/6c8b07df/Elche-Stats",
  "https://fbref.com/en/squads/98e8af82/Rayo-Vallecano-Stats",
  "https://fbref.com/en/squads/2b390eca/Athletic-Club-Stats",
  "https://fbref.com/en/squads/f25da7fb/Celta-Vigo-Stats",
  "https://fbref.com/en/squads/ad2be733/Sevilla-Stats",
  "https://fbref.com/en/squads/e31d1cd9/Real-Sociedad-Stats",
  "https://fbref.com/en/squads/03c57e2b/Osasuna-Stats",
  "https://fbref.com/en/squads/9800b6a1/Levante-Stats",
  "https://fbref.com/en/squads/2aa12281/Mallorca-Stats",
  "https://fbref.com/en/squads/dcc91a7b/Valencia-Stats",
  "https://fbref.com/en/squads/ab358912/Oviedo-Stats",
  "https://fbref.com/en/squads/9024a00a/Girona-Stats",
  "https://fbref.com/en/squads/e2d8892c/Paris-Saint-Germain-Stats",
  "https://fbref.com/en/squads/5725cc7b/Marseille-Stats",
  "https://fbref.com/en/squads/fd4e0f7d/Lens-Stats",
  "https://fbref.com/en/squads/cb188c0c/Lille-Stats",
  "https://fbref.com/en/squads/fd6114db/Monaco-Stats",
  "https://fbref.com/en/squads/d53c0b06/Lyon-Stats",
  "https://fbref.com/en/squads/c0d3eab4/Strasbourg-Stats",
  "https://fbref.com/en/squads/b3072e00/Rennes-Stats",
  "https://fbref.com/en/squads/132ebc33/Nice-Stats",
  "https://fbref.com/en/squads/3f8c4b5f/Toulouse-Stats",
  "https://fbref.com/en/squads/056a5a75/Paris-FC-Stats",
  "https://fbref.com/en/squads/5c2737db/Le-Havre-Stats",
  "https://fbref.com/en/squads/fb08dbb3/Brest-Stats",
  "https://fbref.com/en/squads/69236f98/Angers-Stats",
  "https://fbref.com/en/squads/d7a486cd/Nantes-Stats",
  "https://fbref.com/en/squads/d2c87802/Lorient-Stats",
  "https://fbref.com/en/squads/f83960ae/Metz-Stats",
  "https://fbref.com/en/squads/5ae09109/Auxerre-Stats"
)

length(urls)
unique(urls)
# =============================================================================
# USAGE EXAMPLE
# =============================================================================


# --- Step 2: Scrape Detailed Match Logs ---
# Scrape four different types of match logs for all teams in the 'urls' list.

print("--- STARTING STEP 2: Batch Scrape Detailed Match Logs ---")
print(paste("This will scrape 4 log types for", length(urls), "teams. This is the longest step."))

print("Batch scraping: SHOOTING logs...")
# We wrap each call in 'quote()' so the function can execute it
shooting_logs <- safe_scrape_batch(
  quote(fb_team_match_log_stats_selenium(team_urls = urls, stat_type = "shooting", time_pause = 3)))
print("SHOOTING logs complete. Pausing for 10s.")
Sys.sleep(10)


print("Batch scraping: KEEPER logs...")
keeper_logs <- safe_scrape_batch(
  quote(fb_team_match_log_stats_selenium(team_urls = urls, stat_type = "keeper", time_pause = 3))
)
print("KEEPER logs complete. Pausing for 10s.")
Sys.sleep(10)

print("Batch scraping: PASSING logs...")
passing_logs <- safe_scrape_batch(
  quote(fb_team_match_log_stats_selenium(team_urls = urls, stat_type = "passing", time_pause = 3))
)
print("PASSING logs complete. Pausing for 10s.")
Sys.sleep(10)

print("Batch scraping: GCA logs...")
gca_logs <- safe_scrape_batch(
  quote(fb_team_match_log_stats_selenium(team_urls = urls, stat_type = "gca", time_pause = 3))
)
print("GCA logs complete.")
print("--- STEP 2 Complete. All detailed match logs scraped. ---")


# --- Step 3: Clean and Normalize Shooting Logs (Team-Centric) ---
# Select raw columns based on potential column shifts in the fbref tables.
shooting_logs_1 <- shooting_logs %>%
  # Select all necessary columns, including both possible columns for shifting data
  select(
    Date = X,
    Time = X_2,
    Round = X_4,
    Venue_Raw_A = X_5,          # Shifted Venue column (Ligue 1 teams)
    Venue_Raw_B = X_6,          # Normal Venue column
    Opponent_Raw_A = X_9,       # Shifted Opponent column (Ligue 1 teams)
    Opponent_Raw_B = X_10,      # Normal Opponent column
    Sh_Standard = Standard_1,
    SoT_Standard = Standard_2,
    Dist_Standard = Standard_6,
    FK_Standard = Standard_7,
    PK_Standard = Standard_8,
    ForAgainst = for_against,
    team_url_raw = team_url
  ) %>%
  # Remove the original header rows and any repeated headers in the data
  filter(Date != "Date" & Date != "X") %>%
  # Consolidate shifting columns and extract Team from URL
  mutate(
    # Consolidate Venue: X_6 (normal) takes priority, otherwise use X_5 (shifted)
    Venue = coalesce(Venue_Raw_B, Venue_Raw_A),
    # Consolidate Opponent: X_10 (normal) takes priority, otherwise use X_9 (shifted)
    Opponent = coalesce(Opponent_Raw_B, Opponent_Raw_A),
    # Extract 'Team' from the URL by stripping the prefix and suffix
    Team = str_extract(team_url_raw, "(?<=/)[^/]+(?=-Stats)"),
    Team = str_replace_all(Team, "-", " ")
  ) %>%
  # Convert metric columns to numeric
  mutate(across(c(Sh_Standard, SoT_Standard, Dist_Standard, FK_Standard, PK_Standard), as.numeric))

# 3. Final selection as requested
shooting_logs_1 <- shooting_logs_1 %>%
  select(Team, ForAgainst, Date, Time, Round, Venue, Opponent,
         Sh_Standard, SoT_Standard, Dist_Standard, FK_Standard, PK_Standard)


# --- 5. CREATE AND APPLY MAPPING ---
# Define the mapping to harmonize team names scraped from match logs (Opponent column)
# with the team names extracted from the URL (Team column).
name_mapping <- c(
  # Italian Teams
  "Inter" = "Internazionale",

  # German Teams
  "St. Pauli" = "St Pauli",
  "Köln" = "Koln",
  "Eint Frankfurt" = "Eintracht Frankfurt",
  "Gladbach" = "Monchengladbach",
  "Leverkusen" = "Bayer Leverkusen",

  # English Teams
  "Manchester Utd" = "Manchester United",
  "Nott'ham Forest" = "Nottingham Forest",
  "Newcastle Utd" = "Newcastle United",
  "West Ham" = "West Ham United",
  "Wolves" = "Wolverhampton Wanderers",
  "Tottenham" = "Tottenham Hotspur",
  "Brighton" = "Brighton and Hove Albion",

  # Spanish Teams
  "Atlético Madrid" = "Atletico Madrid",
  "Betis" = "Real Betis",
  "Alavés" = "Alaves",

  # French Teams
  "Paris S-G" = "Paris Saint Germain"
)


# Apply the mapping
# Remove league prefix (e.g., "en ") before applying the team name map
shooting_logs_1 <- shooting_logs_1 %>%
  mutate(Opponent = str_replace(Opponent, "^[a-z]{2,4} ", ""))

# Apply name mapping using recode for safe replacement
shooting_logs_1 <- shooting_logs_1 %>%
  mutate(Opponent = recode(Opponent, !!!name_mapping))



# --- Step 4: Reshape Team-Centric Logs to Match-Centric (Repeated x4) ---
print("--- STARTING STEP 4: Reshape Logs to Match-Centric ---")

# === Reshape Shooting Logs ===
print("Reshaping: SHOOTING logs...")
# Merge the 'For' (Team's own) and 'Against' (Opponent's) log dataframes
# to create a single match row with Home and Away stats.

shooting_logs_1$MatchID <- paste(shooting_logs_1$Team, shooting_logs_1$Date, shooting_logs_1$Opponent, sep = "_")
for_stats <- subset(shooting_logs_1, ForAgainst == "For")
against_stats <- subset(shooting_logs_1, ForAgainst == "Against")

# Rename columns *before* the merge to identify their source (away is 'For', home is 'Against')
names(for_stats) <- sub("Standard", "Standard_away", names(for_stats))
names(against_stats) <- sub("Standard", "Standard_home", names(against_stats))

# Merge by MatchID
merged_stats <- merge(for_stats, against_stats, by = "MatchID")

# Create final match-centric dataframe
shooting_logs_final <- data.frame(
  Date = merged_stats$Date.x,
  Time = merged_stats$Time.x,
  Round = merged_stats$Round.x,
  HomeTeam = merged_stats$Opponent.y, # Opponent from the 'Against' record is HomeTeam
  AwayTeam = merged_stats$Team.x,     # Team from the 'For' record is AwayTeam
  Sh_Standard_home = merged_stats$Sh_Standard_home,
  Sh_Standard_away = merged_stats$Sh_Standard_away,
  SoT_Standard_home = merged_stats$SoT_Standard_home,
  SoT_Standard_away = merged_stats$SoT_Standard_away,
  Dist_Standard_home = merged_stats$Dist_Standard_home,
  Dist_Standard_away = merged_stats$Dist_Standard_away,
  FK_Standard_home = merged_stats$FK_Standard_home,
  FK_Standard_away = merged_stats$FK_Standard_away,
  PK_Standard_home = merged_stats$PK_Standard_home,
  PK_Standard_away = merged_stats$PK_Standard_away
)




# === Reshape Keeper Logs ===
print("Reshaping: KEEPER logs...")
keeper_logs_1 <- keeper_logs %>%
  # 1. Select all potential columns for Venue and Opponent
  select(
    Date = X,
    Time = X_2,
    Round = X_4,
    Venue_Raw_A = X_5,          # Shifted Venue column (e.g., Ligue 1 teams)
    Venue_Raw_B = X_6,          # Normal Venue column
    Opponent_Raw_A = X_9,       # Shifted Opponent column (e.g., Ligue 1 teams)
    Opponent_Raw_B = X_10,      # Normal Opponent column
    PSxG_Performance = Performance_5,
    Stp_percent_Crosses = Crosses_2,
    ForAgainst = for_against,
    team_url_raw = team_url
  ) %>%
  # 2. Remove the original header rows and any repeated headers in the data
  filter(Date != "Date" & Date != "X") %>%
  # 3. Consolidate shifting columns and perform initial mutations
  mutate(
    # Consolidate Venue: Prioritize X_6, otherwise use X_5
    Venue = coalesce(Venue_Raw_B, Venue_Raw_A),
    # Consolidate Opponent: Prioritize X_10, otherwise use X_9
    Opponent = coalesce(Opponent_Raw_B, Opponent_Raw_A),
    # Extract 'Team' from the URL
    Team = str_extract(team_url_raw, "(?<=/)[^/]+(?=-Stats)"),
    Team = str_replace_all(Team, "-", " ")
  ) %>%
  # 4. Convert metric columns to numeric
  mutate(across(c(PSxG_Performance, Stp_percent_Crosses), as.numeric))

# Apply the mapping
keeper_logs_1 <- keeper_logs_1 %>%
  mutate(Opponent = str_replace(Opponent, "^[a-z]{2,4} ", "")) %>%
  mutate(Opponent = recode(Opponent, !!!name_mapping))

# Create MatchID and perform the merge
keeper_logs_1$MatchID <- paste(keeper_logs_1$Team, keeper_logs_1$Date, keeper_logs_1$Opponent, sep = "_")

for_stats_keeper <- subset(keeper_logs_1, ForAgainst == "For")
against_stats_keeper <- subset(keeper_logs_1, ForAgainst == "Against")

# Rename for merge
names(for_stats_keeper) <- sub("PSxG_Performance", "PSxG_Performance_away", names(for_stats_keeper))
names(for_stats_keeper) <- sub("Stp_percent_Crosses", "Stp_percent_Crosses_away", names(for_stats_keeper))
names(against_stats_keeper) <- sub("PSxG_Performance", "PSxG_Performance_home", names(against_stats_keeper))
names(against_stats_keeper) <- sub("Stp_percent_Crosses", "Stp_percent_Crosses_home", names(against_stats_keeper))

merged_stats_keeper <- merge(for_stats_keeper, against_stats_keeper, by = "MatchID")

# Create final keeper log dataframe
keeper_logs_final <- data.frame(
  Date = merged_stats_keeper$Date.x,
  Time = merged_stats_keeper$Time.x,
  Round = merged_stats_keeper$Round.x,
  HomeTeam = merged_stats_keeper$Opponent.y,
  AwayTeam = merged_stats_keeper$Team.x,
  PSxG_Performance_home = merged_stats_keeper$PSxG_Performance_home,
  PSxG_Performance_away = merged_stats_keeper$PSxG_Performance_away,
  Stp_percent_Crosses_home = merged_stats_keeper$Stp_percent_Crosses_home,
  Stp_percent_Crosses_away = merged_stats_keeper$Stp_percent_Crosses_away
)

print("KEEPER logs reshaped.")

# === Reshape Passing Logs ===
print("Reshaping: PASSING logs...")
passing_logs_1 <- passing_logs %>%
  # 1. Select all potential columns for shifting data
  select(
    Date = X, Time = X_2, Round = X_4,

    # Venue/Opponent Shift Fix
    Venue_Raw_A = X_5, Opponent_Raw_A = X_9,
    Venue_Raw_B = X_6, Opponent_Raw_B = X_10,

    # Stable Metrics
    Att_Total = Total_1,
    Cmp_percent_Total = Total_2,

    # Raw columns for the metric shift: Var_27 through Var_32
    V27_Raw = Var_27, # xA
    V28_Raw = Var_28, # KP
    V29_Raw = Var_29, # Final_Third
    V30_Raw = Var_30, # PPA
    V31_Raw = Var_31, # CrsPA
    V32_Raw = Var_32, # PrgP
    
    ForAgainst = for_against,
    team_url_raw = team_url
  ) %>%
  # 2. Remove the original header rows and any repeated headers in the data
  filter(Date != "Date" & Date != "X") %>%

  # --- STEP 3A: CLEAN AND CONVERT ALL RAW METRIC COLUMNS TO NUMERIC ---
  # This handles cases where a numeric column shifts into a non-numeric column header cell ("Match Report")
  mutate(across(
    c(V27_Raw, V28_Raw, V29_Raw, V30_Raw, V31_Raw, V32_Raw, Att_Total, Cmp_percent_Total),
    ~ .x %>%
      na_if("-") %>%
      na_if("") %>%
      as.numeric()
  )) %>%

  # --- STEP 3B: Consolidate shifting columns using coalesce ---
  mutate(
    # Consolidate Venue and Opponent
    Venue = coalesce(Venue_Raw_B, Venue_Raw_A),
    Opponent = coalesce(Opponent_Raw_B, Opponent_Raw_A),

    # Consolidate Metrics (Prioritize Normal column then Shifted column)
    # The Vxx_Raw columns are now all numeric/NA!
    # xA is Var_27. It appears stable, but we use the raw column.
    xA = V27_Raw,

    # KP: Normal is V28_Raw. Shifted is V27_Raw (xA).
    KP = coalesce(V28_Raw, V27_Raw),

    # Final_Third: Normal is V29_Raw. Shifted is V28_Raw (KP).
    Final_Third = coalesce(V29_Raw, V28_Raw),

    # PPA: Normal is V30_Raw. Shifted is V29_Raw (Final_Third).
    PPA = coalesce(V30_Raw, V29_Raw),

    # CrsPA: Normal is V31_Raw. Shifted is V30_Raw (PPA).
    CrsPA = coalesce(V31_Raw, V30_Raw),

    # PrgP: Normal is V32_Raw. Shifted is V31_Raw (CrsPA).
    PrgP = coalesce(V32_Raw, V31_Raw),

    # Extract 'Team' from the URL
    Team = str_extract(team_url_raw, "(?<=/)[^/]+(?=-Stats)"),
    Team = str_replace_all(Team, "-", " ")
  )

# Final selection and reorder
passing_logs_1 <- passing_logs_1 %>%
  select(Team, ForAgainst, Date, Time, Round, Venue, Opponent,
         Att_Total, Cmp_percent_Total, xA, KP, Final_Third, PPA, CrsPA, PrgP)


# Apply name cleaning and mapping
passing_logs_1 <- passing_logs_1 %>%
  mutate(Opponent = str_replace(Opponent, "^[a-z]{2,4} ", "")) %>%
  mutate(Opponent = recode(Opponent, !!!name_mapping))

# Create MatchID and perform the merge
passing_logs_1$MatchID <- paste(passing_logs_1$Team, passing_logs_1$Date, passing_logs_1$Opponent, sep = "_")

for_stats_passing <- subset(passing_logs_1, ForAgainst == "For")
against_stats_passing <- subset(passing_logs_1, ForAgainst == "Against")

# Rename for merge
names(for_stats_passing) <- sub("Att_Total", "Att_Total_away", names(for_stats_passing))
names(for_stats_passing) <- sub("Cmp_percent_Total", "Cmp_percent_Total_away", names(for_stats_passing))
names(for_stats_passing) <- sub("xA", "xA_away", names(for_stats_passing))
names(for_stats_passing) <- sub("KP", "KP_away", names(for_stats_passing))
names(for_stats_passing) <- sub("Final_Third", "Final_Third_away", names(for_stats_passing))
names(for_stats_passing) <- sub("PPA", "PPA_away", names(for_stats_passing))
names(for_stats_passing) <- sub("CrsPA", "CrsPA_away", names(for_stats_passing))
names(for_stats_passing) <- sub("PrgP", "PrgP_away", names(for_stats_passing))

names(against_stats_passing) <- sub("Att_Total", "Att_Total_home", names(against_stats_passing))
names(against_stats_passing) <- sub("Cmp_percent_Total", "Cmp_percent_Total_home", names(against_stats_passing))
names(against_stats_passing) <- sub("xA", "xA_home", names(against_stats_passing))
names(against_stats_passing) <- sub("KP", "KP_home", names(against_stats_passing))
names(against_stats_passing) <- sub("Final_Third", "Final_Third_home", names(against_stats_passing))
names(against_stats_passing) <- sub("PPA", "PPA_home", names(against_stats_passing))
names(against_stats_passing) <- sub("CrsPA", "CrsPA_home", names(against_stats_passing))
names(against_stats_passing) <- sub("PrgP", "PrgP_home", names(against_stats_passing))

merged_stats_passing <- merge(for_stats_passing, against_stats_passing, by = "MatchID")

# Create final passing log dataframe
passing_logs_final <- data.frame(
  Date = merged_stats_passing$Date.x,
  Time = merged_stats_passing$Time.x,
  Round = merged_stats_passing$Round.x,
  HomeTeam = merged_stats_passing$Opponent.y,
  AwayTeam = merged_stats_passing$Team.x,
  Att_Total_home = merged_stats_passing$Att_Total_home,
  Att_Total_away = merged_stats_passing$Att_Total_away,
  Cmp_percent_Total_home = merged_stats_passing$Cmp_percent_Total_home,
  Cmp_percent_Total_away = merged_stats_passing$Cmp_percent_Total_away,
  xA_home = merged_stats_passing$xA_home,
  xA_away = merged_stats_passing$xA_away,
  KP_home = merged_stats_passing$KP_home,
  KP_away = merged_stats_passing$KP_away,
  Final_Third_home = merged_stats_passing$Final_Third_home,
  Final_Third_away = merged_stats_passing$Final_Third_away,
  PPA_home = merged_stats_passing$PPA_home,
  PPA_away = merged_stats_passing$PPA_away,
  CrsPA_home = merged_stats_passing$CrsPA_home,
  CrsPA_away = merged_stats_passing$CrsPA_away,
  PrgP_home = merged_stats_passing$PrgP_home,
  PrgP_away = merged_stats_passing$PrgP_away
)


print("PASSING logs reshaped.")

# === Reshape GCA Logs ===
print("Reshaping: GCA logs...")
gca_logs_1 <- gca_logs %>%
  # 1. Select all potential columns for Venue and Opponent, plus other required columns
  select(
    Date = X,
    Time = X_2,
    Round = X_4,
    Venue_Raw_A = X_5,          # Shifted Venue column
    Venue_Raw_B = X_6,          # Normal Venue column
    Opponent_Raw_A = X_9,       # Shifted Opponent column
    Opponent_Raw_B = X_10,      # Normal Opponent column
    SCA_SCA_Types = SCA_Types,
    GCA_GCA_Types = GCA_Types,
    ForAgainst = for_against,
    team_url_raw = team_url
  ) %>%
  # 2. Remove the original header rows and any repeated headers in the data
  filter(Date != "Date" & Date != "X") %>%
  # 3. Consolidate shifting columns and perform mutations
  mutate(
    # Consolidate Venue: Prioritize X_6 (normal), otherwise use X_5 (shifted)
    Venue = coalesce(Venue_Raw_B, Venue_Raw_A),
    # Consolidate Opponent: Prioritize X_10 (normal), otherwise use X_9 (shifted)
    Opponent = coalesce(Opponent_Raw_B, Opponent_Raw_A),
    # Extract 'Team' from the URL
    Team = str_extract(team_url_raw, "(?<=/)[^/]+(?=-Stats)"),
    Team = str_replace_all(Team, "-", " ")
  ) %>%
  # 4. Convert metric columns to numeric
  mutate(across(c(SCA_SCA_Types, GCA_GCA_Types), as.numeric))
# --- 2. OPPONENT NAME CLEANUP ---
gca_logs_1 <- gca_logs_1 %>%
  # Remove initial league/competition prefixes from opponent names (e.g., "en ")
  mutate(Opponent = str_replace(Opponent, "^[a-z]{2,4} ", ""))

# Apply the mapping
gca_logs_1 <- gca_logs_1 %>%
  mutate(Opponent = recode(Opponent, !!!name_mapping))


# --- 4. DATA SPLIT AND RENAMING ---

# Create a unique Match ID for merging
gca_logs_1$MatchID <- paste(gca_logs_1$Team, gca_logs_1$Date, gca_logs_1$Opponent, sep = "_")

for_stats_gca <- subset(gca_logs_1, ForAgainst == "For")
against_stats_gca <- subset(gca_logs_1, ForAgainst == "Against")

# Rename for merge
names(for_stats_gca) <- sub("SCA_SCA_Types", "SCA_SCA_Types_away", names(for_stats_gca))
names(for_stats_gca) <- sub("GCA_GCA_Types", "GCA_GCA_Types_away", names(for_stats_gca))

names(against_stats_gca) <- sub("SCA_SCA_Types", "SCA_SCA_Types_home", names(against_stats_gca))
names(against_stats_gca) <- sub("GCA_GCA_Types", "GCA_GCA_Types_home", names(against_stats_gca))

merged_stats_gca <- merge(for_stats_gca, against_stats_gca, by = "MatchID")

# Create final GCA log dataframe
gca_logs_final <- data.frame(
  Date = merged_stats_gca$Date.x,
  Time = merged_stats_gca$Time.x,
  Round = merged_stats_gca$Round.x,
  HomeTeam = merged_stats_gca$Opponent.y,
  AwayTeam = merged_stats_gca$Team.x,
  SCA_SCA_Types_home = merged_stats_gca$SCA_SCA_Types_home,
  SCA_SCA_Types_away = merged_stats_gca$SCA_SCA_Types_away,
  GCA_GCA_Types_home = merged_stats_gca$GCA_GCA_Types_home,
  GCA_GCA_Types_away = merged_stats_gca$GCA_GCA_Types_away
)


print("GCA logs reshaped.")
print("--- STEP 4 Complete. All logs reshaped. ---")


# --- Step 5: Process and Merge Data for Each League (Repeated x5) ---
print("--- STARTING STEP 5: Process Each League Individually ---")

# === Process Serie A ===
print("Processing League: Serie A")
print("Scraping Serie A match results (fixtures)...")

# Scrape fixtures using the Selenium-based function
seriea24 <- fb_match_results(country = "ITA", gender = "M", season_end_year = 2026, tier = "1st", time_pause=4)


# Standardize column names and data types for joining
seriea24 <- seriea24 %>%
  rename(
    HomeTeam = Home,
    AwayTeam = Away
  )
seriea24 <- seriea24 %>%
  mutate(Date = as.Date(Date, format = "%Y-%m-%d"))

print("Standardizing Date formats and removing duplicates...")
# Ensure Date types are consistent across all dataframes before joining
keeper_logs_final <- keeper_logs_final %>%
  mutate(Date = as.Date(Date, format = "%Y-%m-%d"))
passing_logs_final <- passing_logs_final %>%
  mutate(Date = as.Date(Date, format = "%Y-%m-%d"))
shooting_logs_final <- shooting_logs_final %>%
  mutate(Date = as.Date(Date, format = "%Y-%m-%d"))
gca_logs_final <- gca_logs_final %>%
  mutate(Date = as.Date(Date, format = "%Y-%m-%d"))

# Remove duplicate match logs (if any)
keeper_logs_final <- keeper_logs_final %>% distinct(Date, HomeTeam, AwayTeam, .keep_all = TRUE)
passing_logs_final <- passing_logs_final %>% distinct(Date, HomeTeam, AwayTeam, .keep_all = TRUE)
shooting_logs_final <- shooting_logs_final %>% distinct(Date, HomeTeam, AwayTeam, .keep_all = TRUE)
gca_logs_final <- gca_logs_final %>% distinct(Date, HomeTeam, AwayTeam, .keep_all = TRUE)

# Normalize team names to match across fixture list and logs (reverse a previous map for Internazionale)
print("Normalizing Serie A team names (Internazionale -> Inter)...")
seriea24 <- seriea24 %>% mutate(HomeTeam = ifelse(HomeTeam == "Internazionale", "Inter", HomeTeam),
                                AwayTeam = ifelse(AwayTeam == "Internazionale", "Inter", AwayTeam))
keeper_logs_final <- keeper_logs_final %>%
  mutate(HomeTeam = ifelse(HomeTeam == "Internazionale", "Inter", HomeTeam),
         AwayTeam = ifelse(AwayTeam == "Internazionale", "Inter", AwayTeam))
passing_logs_final <- passing_logs_final %>% mutate(HomeTeam = ifelse(HomeTeam == "Internazionale", "Inter", HomeTeam),
                                                  AwayTeam = ifelse(AwayTeam == "Internazionale", "Inter", AwayTeam))
shooting_logs_final <- shooting_logs_final %>% mutate(HomeTeam = ifelse(HomeTeam == "Internazionale", "Inter", HomeTeam),
                                                    AwayTeam = ifelse(AwayTeam == "Internazionale", "Inter", AwayTeam))
gca_logs_final <- gca_logs_final %>% mutate(HomeTeam = ifelse(HomeTeam == "Internazionale", "Inter", HomeTeam),
                                          AwayTeam = ifelse(AwayTeam == "Internazionale", "Inter", AwayTeam))


# Join all datasets for Serie A
print("Joining Serie A fixtures with all 4 log dataframes...")
merged_data <- seriea24 %>%
  left_join(keeper_logs_final, by = c("Date", "HomeTeam", "AwayTeam")) %>%
  left_join(passing_logs_final, by = c("Date", "HomeTeam", "AwayTeam")) %>%
  left_join(shooting_logs_final, by = c("Date", "HomeTeam", "AwayTeam")) %>%
  left_join(gca_logs_final, by = c("Date", "HomeTeam", "AwayTeam"))

# Remove any duplicate rows resulting from the joins
merged_data <- merged_data %>% distinct()

# Select the final columns
print("Selecting final columns for Serie A.")
seriea25 <- merged_data %>% select(Season_End_Year, Wk, Date, Time.x, venue, referee, HomeTeam, AwayTeam,
  HomeGoals, AwayGoals, Home_xG, Away_xG, PSxG_Performance_home, PSxG_Performance_away,
  Stp_percent_Crosses_home, Stp_percent_Crosses_away, Att_Total_home, Att_Total_away,
  Cmp_percent_Total_home, Cmp_percent_Total_away,
  xA_home, xA_away, KP_home, KP_away, Final_Third_home, Final_Third_away,
  PPA_home, PPA_away, CrsPA_home, CrsPA_away, PrgP_home, PrgP_away,
  Sh_Standard_home, Sh_Standard_away, SoT_Standard_home, SoT_Standard_away,
  Dist_Standard_home, Dist_Standard_away, FK_Standard_home, FK_Standard_away,
  PK_Standard_home, PK_Standard_away, SCA_SCA_Types_home, SCA_SCA_Types_away,
  GCA_GCA_Types_home, GCA_GCA_Types_away
)

print("Serie A processing complete.")

# === Process La Liga ===
print("Processing League: La Liga")
print("Scraping La Liga match results (fixtures)...")
liga <- fb_match_results(country = "ESP", gender = "M", season_end_year = 2026, tier = "1st", time_pause=4)

# Normalize team names, including removing Spanish accents using stringi::stri_trans_general
print("Normalizing La Liga team names (removing accents, standardizing Betis)...")
liga$HomeTeam <- stringi::stri_trans_general(liga$HomeTeam, "Latin-ASCII")
liga$AwayTeam <- stringi::stri_trans_general(liga$AwayTeam, "Latin-ASCII")
shooting_logs_final$HomeTeam <- stringi::stri_trans_general(shooting_logs_final$HomeTeam, "Latin-ASCII")
shooting_logs_final$AwayTeam <- stringi::stri_trans_general(shooting_logs_final$AwayTeam, "Latin-ASCII")
keeper_logs_final$HomeTeam <- stringi::stri_trans_general(keeper_logs_final$HomeTeam, "Latin-ASCII")
keeper_logs_final$AwayTeam <- stringi::stri_trans_general(keeper_logs_final$AwayTeam, "Latin-ASCII")
passing_logs_final$HomeTeam <- stringi::stri_trans_general(passing_logs_final$HomeTeam, "Latin-ASCII")
passing_logs_final$AwayTeam <- stringi::stri_trans_general(passing_logs_final$AwayTeam, "Latin-ASCII")
gca_logs_final$HomeTeam <- stringi::stri_trans_general(gca_logs_final$HomeTeam, "Latin-ASCII")
gca_logs_final$AwayTeam <- stringi::stri_trans_general(gca_logs_final$AwayTeam, "Latin-ASCII")

# Further manual normalization for La Liga names (Betis is often 'Real Betis' in logs)
keeper_logs_final$HomeTeam <- gsub("Real Betis", "Betis", keeper_logs_final$HomeTeam)
keeper_logs_final$AwayTeam <- gsub("Real Betis", "Betis", keeper_logs_final$AwayTeam)
passing_logs_final$HomeTeam <- gsub("Real Betis", "Betis", passing_logs_final$HomeTeam)
passing_logs_final$AwayTeam <- gsub("Real Betis", "Betis", passing_logs_final$AwayTeam)
shooting_logs_final$HomeTeam <- gsub("Real Betis", "Betis", shooting_logs_final$HomeTeam)
shooting_logs_final$AwayTeam <- gsub("Real Betis", "Betis", shooting_logs_final$AwayTeam)
gca_logs_final$HomeTeam <- gsub("Real Betis", "Betis", gca_logs_final$HomeTeam)
gca_logs_final$AwayTeam <- gsub("Real Betis", "Betis", gca_logs_final$AwayTeam)
shooting_logs_final$AwayTeam <- gsub("Deportivo La Coruna", "La Coruna", shooting_logs_final$AwayTeam)
keeper_logs_final$AwayTeam <- gsub("Deportivo La Coruna", "La Coruna", keeper_logs_final$AwayTeam)
passing_logs_final$AwayTeam <- gsub("Deportivo La Coruna", "La Coruna", passing_logs_final$AwayTeam)
gca_logs_final$AwayTeam <- gsub("Deportivo La Coruna", "La Coruna", gca_logs_final$AwayTeam)

# Join all datasets for La Liga
print("Joining La Liga fixtures with all 4 log dataframes...")
merged_data <- liga %>%
  # Ensure Date type for merging
  mutate(Date = as.Date(Date, format = "%Y-%m-%d")) %>%

  # Perform the joins, ensuring log dataframes also have consistent Date format
  left_join(keeper_logs_final %>%
              mutate(Date = as.Date(Date, format = "%Y-%m-%d")),
            by = c("Date", "HomeTeam", "AwayTeam")) %>%
  left_join(passing_logs_final %>%
              mutate(Date = as.Date(Date, format = "%Y-%m-%d")),
            by = c("Date", "HomeTeam", "AwayTeam")) %>%
  left_join(shooting_logs_final %>%
              mutate(Date = as.Date(Date, format = "%Y-%m-%d")),
            by = c("Date", "HomeTeam", "AwayTeam")) %>%
  left_join(gca_logs_final %>%
              mutate(Date = as.Date(Date, format = "%Y-%m-%d")),
            by = c("Date", "HomeTeam", "AwayTeam"))

# Select the final columns
print("Selecting final columns for La Liga.")
liga <- merged_data %>% select(
  Season_End_Year, Wk, Date, Time.x, venue, referee, HomeTeam, AwayTeam,
  HomeGoals, AwayGoals, Home_xG, Away_xG, PSxG_Performance_home, PSxG_Performance_away,
  Stp_percent_Crosses_home, Stp_percent_Crosses_away, Att_Total_home, Att_Total_away,
  Cmp_percent_Total_home, Cmp_percent_Total_away,
  xA_home, xA_away, KP_home, KP_away, Final_Third_home, Final_Third_away,
  PPA_home, PPA_away, CrsPA_home, CrsPA_away, PrgP_home, PrgP_away,
  Sh_Standard_home, Sh_Standard_away, SoT_Standard_home, SoT_Standard_away,
  Dist_Standard_home, Dist_Standard_away, FK_Standard_home, FK_Standard_away,
  PK_Standard_home, PK_Standard_away, SCA_SCA_Types_home, SCA_SCA_Types_away,
  GCA_GCA_Types_home, GCA_GCA_Types_away
)

print("La Liga processing complete.")

# === Process Bundesliga ===
print("Processing League: Bundesliga")
print("Scraping Bundesliga match results (fixtures)...")
bundesliga <- fb_match_results(country = "GER", gender = "M", season_end_year = 2026, tier = "1st", time_pause=4)
# Manual normalization for German team names (special characters, common names)
print("Normalizing Bundesliga team names (M'Gladbach, Köln, etc.)...")
bundesliga <- bundesliga %>%
  mutate(
    # Change 'Home' to 'HomeTeam'
    HomeTeam = case_when(
      HomeTeam == "Gladbach" ~ "M'Gladbach",
      HomeTeam == "Monchengladbach" ~ "M'Gladbach",
      HomeTeam == "Koln" ~ "Köln",
      HomeTeam == "Nurnberg" ~ "Nürnberg",
      HomeTeam == "Bayer Leverkusen" ~ "Leverkusen",
      HomeTeam == "Eintracht Frankfurt" ~ "Eint Frankfurt",
      HomeTeam == "St. Pauli" ~ "St Pauli",
      HomeTeam == "Dusseldorf" ~ "Düsseldorf",
      TRUE ~ HomeTeam # Ensure the base variable is also HomeTeam here
    ),
    # Change 'Away' to 'AwayTeam'
    AwayTeam = case_when(
      AwayTeam == "Gladbach" ~ "M'Gladbach",
      AwayTeam == "Monchengladbach" ~ "M'Gladbach",
      AwayTeam == "Koln" ~ "Köln",
      AwayTeam == "Nurnberg" ~ "Nürnberg",
      AwayTeam == "Bayer Leverkusen" ~ "Leverkusen",
      AwayTeam == "Eintracht Frankfurt" ~ "Eint Frankfurt",
      AwayTeam == "Dusseldorf" ~ "Düsseldorf",
      AwayTeam == "St. Pauli" ~ "St Pauli",
      TRUE ~ AwayTeam # Ensure the base variable is also AwayTeam here
    )
  )
# Apply the same normalization to all log files
shooting_logs_final <- shooting_logs_final %>%
  mutate(
    HomeTeam = case_when(
      HomeTeam == "Gladbach" ~ "M'Gladbach",
      HomeTeam == "Monchengladbach" ~ "M'Gladbach",
      HomeTeam == "Koln" ~ "Köln",
      HomeTeam == "Nurnberg" ~ "Nürnberg",
      HomeTeam == "Bayer Leverkusen" ~ "Leverkusen",
      HomeTeam == "Dusseldorf" ~ "Düsseldorf",
      HomeTeam == "Greuther Furth" ~ "Greuther Fürth",
      HomeTeam == "Eintracht Frankfurt" ~ "Eint Frankfurt",
      HomeTeam == "St. Pauli" ~ "St Pauli",
      HomeTeam == "Dusseldorf" ~ "Düsseldorf",
    TRUE ~ HomeTeam
    ),
    AwayTeam = case_when(
      AwayTeam == "Gladbach" ~ "M'Gladbach",
      AwayTeam == "Monchengladbach" ~ "M'Gladbach",
      AwayTeam == "Koln" ~ "Köln",
      AwayTeam == "Nurnberg" ~ "Nürnberg",
      AwayTeam == "Bayer Leverkusen" ~ "Leverkusen",
      AwayTeam == "Eintracht Frankfurt" ~ "Eint Frankfurt",
      AwayTeam  == "Dusseldorf" ~ "Düsseldorf",
      AwayTeam == "Greuther Furth" ~ "Greuther Fürth",
    TRUE ~ AwayTeam
    )
  )
keeper_logs_final <- keeper_logs_final %>%
  mutate(
    HomeTeam = case_when(
      HomeTeam == "Gladbach" ~ "M'Gladbach",
      HomeTeam == "Monchengladbach" ~ "M'Gladbach",
      HomeTeam == "Koln" ~ "Köln",
      HomeTeam == "Nurnberg" ~ "Nürnberg",
      HomeTeam == "Bayer Leverkusen" ~ "Leverkusen",
      HomeTeam == "Dusseldorf" ~ "Düsseldorf",
      HomeTeam == "Greuther Furth" ~ "Greuther Fürth",
      HomeTeam == "Eintracht Frankfurt" ~ "Eint Frankfurt",
      HomeTeam == "St. Pauli" ~ "St Pauli",
      HomeTeam == "Dusseldorf" ~ "Düsseldorf",
    TRUE ~ HomeTeam
    ),
    AwayTeam = case_when(
      AwayTeam == "Gladbach" ~ "M'Gladbach",
      AwayTeam == "Monchengladbach" ~ "M'Gladbach",
      AwayTeam == "Koln" ~ "Köln",
      AwayTeam == "Nurnberg" ~ "Nürnberg",
      AwayTeam == "Bayer Leverkusen" ~ "Leverkusen",
      AwayTeam == "Eintracht Frankfurt" ~ "Eint Frankfurt",
      AwayTeam  == "Dusseldorf" ~ "Düsseldorf",
      AwayTeam == "Greuther Furth" ~ "Greuther Fürth",
    TRUE ~ AwayTeam
    )
  )
gca_logs_final <- gca_logs_final %>%
  mutate(
    HomeTeam = case_when(
      HomeTeam == "Gladbach" ~ "M'Gladbach",
      HomeTeam == "Monchengladbach" ~ "M'Gladbach",
      HomeTeam == "Koln" ~ "Köln",
      HomeTeam == "Nurnberg" ~ "Nürnberg",
      HomeTeam == "Bayer Leverkusen" ~ "Leverkusen",
      HomeTeam == "Dusseldorf" ~ "Düsseldorf",
      HomeTeam == "Greuther Furth" ~ "Greuther Fürth",
      HomeTeam == "Eintracht Frankfurt" ~ "Eint Frankfurt",
      HomeTeam == "St. Pauli" ~ "St Pauli",
      HomeTeam == "Dusseldorf" ~ "Düsseldorf",
    TRUE ~ HomeTeam
    ),
    AwayTeam = case_when(
      AwayTeam == "Gladbach" ~ "M'Gladbach",
      AwayTeam == "Monchengladbach" ~ "M'Gladbach",
      AwayTeam == "Koln" ~ "Köln",
      AwayTeam == "Nurnberg" ~ "Nürnberg",
      AwayTeam == "Bayer Leverkusen" ~ "Leverkusen",
      AwayTeam == "Eintracht Frankfurt" ~ "Eint Frankfurt",
      AwayTeam  == "Dusseldorf" ~ "Düsseldorf",
      AwayTeam == "Greuther Furth" ~ "Greuther Fürth",
    TRUE ~ AwayTeam
    )
  )
passing_logs_final <- passing_logs_final %>%
  mutate(
    HomeTeam = case_when(
      HomeTeam == "Gladbach" ~ "M'Gladbach",
      HomeTeam == "Monchengladbach" ~ "M'Gladbach",
      HomeTeam == "Koln" ~ "Köln",
      HomeTeam == "Nurnberg" ~ "Nürnberg",
      HomeTeam == "Bayer Leverkusen" ~ "Leverkusen",
      HomeTeam == "Dusseldorf" ~ "Düsseldorf",
      HomeTeam == "Greuther Furth" ~ "Greuther Fürth",
      HomeTeam == "Eintracht Frankfurt" ~ "Eint Frankfurt",
      HomeTeam == "St. Pauli" ~ "St Pauli",
      HomeTeam == "Dusseldorf" ~ "Düsseldorf",
    TRUE ~ HomeTeam
    ),
    AwayTeam = case_when(
      AwayTeam == "Gladbach" ~ "M'Gladbach",
      AwayTeam == "Monchengladbach" ~ "M'Gladbach",
      AwayTeam == "Koln" ~ "Köln",
      AwayTeam == "Nurnberg" ~ "Nürnberg",
      AwayTeam == "Bayer Leverkusen" ~ "Leverkusen",
      AwayTeam == "Eintracht Frankfurt" ~ "Eint Frankfurt",
      AwayTeam == "Dusseldorf" ~ "Düsseldorf",
      AwayTeam == "Greuther Furth" ~ "Greuther Fürth",
    TRUE ~ AwayTeam
    )
  )
print("Bundesliga normalization complete. Removing duplicates...")


# Join all datasets for Bundesliga
print("Joining Bundesliga fixtures with all 4 log dataframes...")
merged_data <- bundesliga %>%
  # Ensure Date type for merging
  mutate(Date = as.Date(Date, format = "%Y-%m-%d")) %>%

  # Perform the joins, ensuring log dataframes also have consistent Date format
  left_join(keeper_logs_final %>%
              mutate(Date = as.Date(Date, format = "%Y-%m-%d")),
            by = c("Date", "HomeTeam", "AwayTeam")) %>%
  left_join(passing_logs_final %>%
              mutate(Date = as.Date(Date, format = "%Y-%m-%d")),
            by = c("Date", "HomeTeam", "AwayTeam")) %>%
  left_join(shooting_logs_final %>%
              mutate(Date = as.Date(Date, format = "%Y-%m-%d")),
            by = c("Date", "HomeTeam", "AwayTeam")) %>%
  left_join(gca_logs_final %>%
              mutate(Date = as.Date(Date, format = "%Y-%m-%d")),
            by = c("Date", "HomeTeam", "AwayTeam"))

# Select the final columns
print("Selecting final columns for Bundesliga.")
bundesliga <- merged_data %>% select(
  Season_End_Year, Wk, Date, Time.x, venue, referee, HomeTeam, AwayTeam,
  HomeGoals, AwayGoals, Home_xG, Away_xG, PSxG_Performance_home, PSxG_Performance_away,
  Stp_percent_Crosses_home, Stp_percent_Crosses_away, Att_Total_home, Att_Total_away,
  Cmp_percent_Total_home, Cmp_percent_Total_away,
  xA_home, xA_away, KP_home, KP_away, Final_Third_home, Final_Third_away,
  PPA_home, PPA_away, CrsPA_home, CrsPA_away, PrgP_home, PrgP_away,
  Sh_Standard_home, Sh_Standard_away, SoT_Standard_home, SoT_Standard_away,
  Dist_Standard_home, Dist_Standard_away, FK_Standard_home, FK_Standard_away,
  PK_Standard_home, PK_Standard_away, SCA_SCA_Types_home, SCA_SCA_Types_away,
  GCA_GCA_Types_home, GCA_GCA_Types_away
)

print("Bundesliga processing complete.")


# === Process Premier League ===
print("Processing League: Premier League")
print("Scraping Premier League match results (fixtures)...")
pl <- fb_match_results(country = "ENG", gender = "M", season_end_year = 2026, tier = "1st", time_pause=4)

# Manual normalization for English team names
print("Normalizing Premier League team names (Manchester Utd, Spurs, etc.)...")
shooting_logs_final <- shooting_logs_final %>%
  mutate(
    HomeTeam = case_when(
      HomeTeam == "Brighton and Hove Albion" ~ "Brighton",
      HomeTeam == "Huddersfield Town" ~ "Huddersfield",
      HomeTeam == "Manchester United" ~ "Manchester Utd",
      HomeTeam == "Newcastle United" ~ "Newcastle Utd",
      HomeTeam == "Nottingham Forest" ~ "Nott'ham Forest",
      HomeTeam == "Sheffield United" ~ "Sheffield Utd",
      HomeTeam == "Tottenham Hotspur" ~ "Tottenham",
      HomeTeam == "West Bromwich Albion" ~ "West Brom",
      HomeTeam == "West Ham United" ~ "West Ham",
      HomeTeam == "Wolverhampton Wanderers" ~ "Wolves",
    TRUE ~ HomeTeam
    ),
    AwayTeam = case_when(
      AwayTeam == "Brighton and Hove Albion" ~ "Brighton",
      AwayTeam == "Huddersfield Town" ~ "Huddersfield",
      AwayTeam == "Manchester United" ~ "Manchester Utd",
      AwayTeam == "Newcastle United" ~ "Newcastle Utd",
      AwayTeam == "Nottingham Forest" ~ "Nott'ham Forest",
      AwayTeam == "Sheffield United" ~ "Sheffield Utd",
      AwayTeam == "Tottenham Hotspur" ~ "Tottenham",
      AwayTeam == "West Bromwich Albion" ~ "West Brom",
      AwayTeam == "West Ham United" ~ "West Ham",
      AwayTeam == "Wolverhampton Wanderers" ~ "Wolves",
    TRUE ~ AwayTeam
    )
  )
keeper_logs_final <- keeper_logs_final %>%
  mutate(
    HomeTeam = case_when(
      HomeTeam == "Brighton and Hove Albion" ~ "Brighton",
      HomeTeam == "Huddersfield Town" ~ "Huddersfield",
      HomeTeam == "Manchester United" ~ "Manchester Utd",
      HomeTeam == "Newcastle United" ~ "Newcastle Utd",
      HomeTeam == "Nottingham Forest" ~ "Nott'ham Forest",
      HomeTeam == "Sheffield United" ~ "Sheffield Utd",
      HomeTeam == "Tottenham Hotspur" ~ "Tottenham",
      HomeTeam == "West Bromwich Albion" ~ "West Brom",
      HomeTeam == "West Ham United" ~ "West Ham",
      HomeTeam == "Wolverhampton Wanderers" ~ "Wolves",
    TRUE ~ HomeTeam
    ),
    AwayTeam = case_when(
      AwayTeam == "Brighton and Hove Albion" ~ "Brighton",
      AwayTeam == "Huddersfield Town" ~ "Huddersfield",
      AwayTeam == "Manchester United" ~ "Manchester Utd",
      AwayTeam == "Newcastle United" ~ "Newcastle Utd",
      AwayTeam == "Nottingham Forest" ~ "Nott'ham Forest",
      AwayTeam == "Sheffield United" ~ "Sheffield Utd",
      AwayTeam == "Tottenham Hotspur" ~ "Tottenham",
      AwayTeam == "West Bromwich Albion" ~ "West Brom",
      AwayTeam == "West Ham United" ~ "West Ham",
      AwayTeam == "Wolverhampton Wanderers" ~ "Wolves",
    TRUE ~ AwayTeam
    )
  )
passing_logs_final <- passing_logs_final %>%
  mutate(
    HomeTeam = case_when(
      HomeTeam == "Brighton and Hove Albion" ~ "Brighton",
      HomeTeam == "Huddersfield Town" ~ "Huddersfield",
      HomeTeam == "Manchester United" ~ "Manchester Utd",
      HomeTeam == "Newcastle United" ~ "Newcastle Utd",
      HomeTeam == "Nottingham Forest" ~ "Nott'ham Forest",
      HomeTeam == "Sheffield United" ~ "Sheffield Utd",
      HomeTeam == "Tottenham Hotspur" ~ "Tottenham",
      HomeTeam == "West Bromwich Albion" ~ "West Brom",
      HomeTeam == "West Ham United" ~ "West Ham",
      HomeTeam == "Wolverhampton Wanderers" ~ "Wolves",
    TRUE ~ HomeTeam
    ),
    AwayTeam = case_when(
      AwayTeam == "Brighton and Hove Albion" ~ "Brighton",
      AwayTeam == "Huddersfield Town" ~ "Huddersfield",
      AwayTeam == "Manchester United" ~ "Manchester Utd",
      AwayTeam == "Newcastle United" ~ "Newcastle Utd",
      AwayTeam == "Nottingham Forest" ~ "Nott'ham Forest",
      AwayTeam == "Sheffield United" ~ "Sheffield Utd",
      AwayTeam == "Tottenham Hotspur" ~ "Tottenham",
      AwayTeam == "West Bromwich Albion" ~ "West Brom",
      AwayTeam == "West Ham United" ~ "West Ham",
      AwayTeam == "Wolverhampton Wanderers" ~ "Wolves",
    TRUE ~ AwayTeam
    )
  )
gca_logs_final <- gca_logs_final %>%
  mutate(
    HomeTeam = case_when(
      HomeTeam == "Brighton and Hove Albion" ~ "Brighton",
      HomeTeam == "Huddersfield Town" ~ "Huddersfield",
      HomeTeam == "Manchester United" ~ "Manchester Utd",
      HomeTeam == "Newcastle United" ~ "Newcastle Utd",
      HomeTeam == "Nottingham Forest" ~ "Nott'ham Forest",
      HomeTeam == "Sheffield United" ~ "Sheffield Utd",
      HomeTeam == "Tottenham Hotspur" ~ "Tottenham",
      HomeTeam == "West Bromwich Albion" ~ "West Brom",
      HomeTeam == "West Ham United" ~ "West Ham",
      HomeTeam == "Wolverhampton Wanderers" ~ "Wolves",
    TRUE ~ HomeTeam
    ),
    AwayTeam = case_when(
      AwayTeam == "Brighton and Hove Albion" ~ "Brighton",
      AwayTeam == "Huddersfield Town" ~ "Huddersfield",
      AwayTeam == "Manchester United" ~ "Manchester Utd",
      AwayTeam == "Newcastle United" ~ "Newcastle Utd",
      AwayTeam == "Nottingham Forest" ~ "Nott'ham Forest",
      AwayTeam == "Sheffield United" ~ "Sheffield Utd",
      AwayTeam == "Tottenham Hotspur" ~ "Tottenham",
      AwayTeam == "West Bromwich Albion" ~ "West Brom",
      AwayTeam == "West Ham United" ~ "West Ham",
      AwayTeam == "Wolverhampton Wanderers" ~ "Wolves",
    TRUE ~ AwayTeam
    )
  )

# Join all datasets for Premier League
print("Joining Premier League fixtures with all 4 log dataframes...")
merged_data <- pl %>%
  # Ensure Date type for merging
  mutate(Date = as.Date(Date, format = "%Y-%m-%d")) %>%

  # Perform the joins
  left_join(keeper_logs_final %>%
              mutate(Date = as.Date(Date, format = "%Y-%m-%d")),
            by = c("Date", "HomeTeam", "AwayTeam")) %>%
  left_join(passing_logs_final %>%
              mutate(Date = as.Date(Date, format = "%Y-%m-%d")),
            by = c("Date", "HomeTeam", "AwayTeam")) %>%
  left_join(shooting_logs_final %>%
              mutate(Date = as.Date(Date, format = "%Y-%m-%d")),
            by = c("Date", "HomeTeam", "AwayTeam")) %>%
  left_join(gca_logs_final %>%
              mutate(Date = as.Date(Date, format = "%Y-%m-%d")),
            by = c("Date", "HomeTeam", "AwayTeam"))

# Select the final columns
print("Selecting final columns for Premier League.")
premier_league <- merged_data %>% select(
  Season_End_Year, Wk, Date, Time.x, venue, referee, HomeTeam, AwayTeam,
  HomeGoals, AwayGoals, Home_xG, Away_xG, PSxG_Performance_home, PSxG_Performance_away,
  Stp_percent_Crosses_home, Stp_percent_Crosses_away, Att_Total_home, Att_Total_away,
  Cmp_percent_Total_home, Cmp_percent_Total_away,
  xA_home, xA_away, KP_home, KP_away, Final_Third_home, Final_Third_away,
  PPA_home, PPA_away, CrsPA_home, CrsPA_away, PrgP_home, PrgP_away,
  Sh_Standard_home, Sh_Standard_away, SoT_Standard_home, SoT_Standard_away,
  Dist_Standard_home, Dist_Standard_away, FK_Standard_home, FK_Standard_away,
  PK_Standard_home, PK_Standard_away, SCA_SCA_Types_home, SCA_SCA_Types_away,
  GCA_GCA_Types_home, GCA_GCA_Types_away
)

print("Premier League processing complete.")

# === Process Ligue 1 ===
print("Processing League: Ligue 1")
print("Scraping Ligue 1 match results (fixtures)...")
ligue1 <- fb_match_results(country = "FRA", gender = "M", season_end_year = 2026, tier = "1st", time_pause=4)

# Manual normalization for French team names
print("Normalizing Ligue 1 team names (Paris S-G, Saint-Étienne)...")
shooting_logs_final <- shooting_logs_final %>%
  mutate(
    HomeTeam = case_when(
      HomeTeam == "Paris Saint Germain" ~ "Paris S-G",
      HomeTeam == "Saint Etienne" ~ "Saint-Étienne",
      HomeTeam == "Saint-Etienne" ~ "Saint-Étienne",
    TRUE ~ HomeTeam
    ),
    AwayTeam = case_when(
      AwayTeam == "Paris Saint Germain" ~ "Paris S-G",
      AwayTeam == "Saint Etienne" ~ "Saint-Étienne",
      AwayTeam == "Saint-Etienne" ~ "Saint-Étienne",
    TRUE ~ AwayTeam
    )
  )
keeper_logs_final <- keeper_logs_final %>%
  mutate(
    HomeTeam = case_when(
      HomeTeam == "Paris Saint Germain" ~ "Paris S-G",
      HomeTeam == "Saint Etienne" ~ "Saint-Étienne",
      HomeTeam == "Saint-Etienne" ~ "Saint-Étienne",
    TRUE ~ HomeTeam
    ),
    AwayTeam = case_when(
      AwayTeam == "Paris Saint Germain" ~ "Paris S-G",
      AwayTeam == "Saint Etienne" ~ "Saint-Étienne",
      AwayTeam == "Saint-Etienne" ~ "Saint-Étienne",
    TRUE ~ AwayTeam
    )
  )
passing_logs_final <- passing_logs_final %>%
  mutate(
    HomeTeam = case_when(
      HomeTeam == "Paris Saint Germain" ~ "Paris S-G",
      HomeTeam == "Saint Etienne" ~ "Saint-Étienne",
      HomeTeam == "Saint-Etienne" ~ "Saint-Étienne",
    TRUE ~ HomeTeam
    ),
    AwayTeam = case_when(
      AwayTeam == "Paris Saint Germain" ~ "Paris S-G",
      AwayTeam == "Saint Etienne" ~ "Saint-Étienne",
      AwayTeam == "Saint-Etienne" ~ "Saint-Étienne",
    TRUE ~ AwayTeam
    )
  )
gca_logs_final <- gca_logs_final %>%
  mutate(
    HomeTeam = case_when(
      HomeTeam == "Paris Saint Germain" ~ "Paris S-G",
      HomeTeam == "Saint Etienne" ~ "Saint-Étienne",
      HomeTeam == "Saint-Etienne" ~ "Saint-Étienne",
    TRUE ~ HomeTeam
    ),
    AwayTeam = case_when(
      AwayTeam == "Paris Saint Germain" ~ "Paris S-G",
      AwayTeam == "Saint Etienne" ~ "Saint-Étienne",
      AwayTeam == "Saint-Etienne" ~ "Saint-Étienne",
    TRUE ~ AwayTeam
    )
  )

# Join all datasets for Ligue 1
print("Joining Ligue 1 fixtures with all 4 log dataframes...")
merged_data <- ligue1 %>%
  # Ensure Date type for merging
  mutate(Date = as.Date(Date, format = "%Y-%m-%d")) %>%

  # Perform the joins
  left_join(keeper_logs_final %>%
              mutate(Date = as.Date(Date, format = "%Y-%m-%d")),
            by = c("Date", "HomeTeam", "AwayTeam")) %>%
  left_join(passing_logs_final %>%
              mutate(Date = as.Date(Date, format = "%Y-%m-%d")),
            by = c("Date", "HomeTeam", "AwayTeam")) %>%
  left_join(shooting_logs_final %>%
              mutate(Date = as.Date(Date, format = "%Y-%m-%d")),
            by = c("Date", "HomeTeam", "AwayTeam")) %>%
  left_join(gca_logs_final %>%
              mutate(Date = as.Date(Date, format = "%Y-%m-%d")),
            by = c("Date", "HomeTeam", "AwayTeam"))

# Select the final columns
print("Selecting final columns for Ligue 1.")
ligue1 <- merged_data %>% select(
  Season_End_Year, Wk, Date, Time.x, venue, referee, HomeTeam, AwayTeam,
  HomeGoals, AwayGoals, Home_xG, Away_xG, PSxG_Performance_home, PSxG_Performance_away,
  Stp_percent_Crosses_home, Stp_percent_Crosses_away, Att_Total_home, Att_Total_away,
  Cmp_percent_Total_home, Cmp_percent_Total_away,
  xA_home, xA_away, KP_home, KP_away, Final_Third_home, Final_Third_away,
  PPA_home, PPA_away, CrsPA_home, CrsPA_away, PrgP_home, PrgP_away,
  Sh_Standard_home, Sh_Standard_away, SoT_Standard_home, SoT_Standard_away,
  Dist_Standard_home, Dist_Standard_away, FK_Standard_home, FK_Standard_away,
  PK_Standard_home, PK_Standard_away, SCA_SCA_Types_home, SCA_SCA_Types_away,
  GCA_GCA_Types_home, GCA_GCA_Types_away
)

print("Ligue 1 processing complete.")
print("--- STEP 5 Complete. All leagues processed and normalized. ---")


# --- Step 6: Final Aggregation and Output ---
print("--- STARTING STEP 6: Final Aggregation and Output ---")

# Add a 'div' (division) column to identify the league for each match
print("Adding 'div' column to each league dataframe...")
seriea25$div <- 'Serie A'
liga$div <- 'La Liga'
bundesliga$div <- 'Bundesliga'
premier_league$div <- 'Premier League'
ligue1$div <- 'Ligue 1'

# Combine all 5 league dataframes into one master dataframe
print("Binding all 5 league dataframes into 'europa26'...")
europa26 <- bind_rows(premier_league, liga, ligue1, seriea25, bundesliga)
# Write the final, combined dataset to a CSV file for the Python script
print("Writing final CSV to 'data/europa26.csv'...")
# Create the directory if it doesn't exist
if (!dir.exists("data")) {
  dir.create("data")
  print("Created 'data' directory.")
}

write.csv(europa26, "data/europa26.csv", row.names = FALSE)

print("--- SCRIPT COMPLETE ---")
print(paste("Final dataset 'europa26' created with", nrow(europa26), "rows."))
print("CSV file 'data/europa26.csv' has been successfully written.")