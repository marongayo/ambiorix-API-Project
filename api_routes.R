library(ambiorix)
library(RSQLite)
library(jsonlite)
library(data.table)



flights_dt <- (nycflights13::flights)
flights_dt <- as.data.table(nycflights13::flights)
flights_dt[, delayed := ifelse(is.na(dep_delay), NA, dep_delay > 15)]

avg_delay <- flights_dt[, .(avg_dep_delay = mean(dep_delay, na.rm = TRUE)), by = carrier]
top_destinations <- flights_dt[, .N, by = dest][order(-N)]


#creating a SQLite db to dump the processed data
conn <- dbConnect(SQLite(), "flights.db")
dbExecute(conn, "DROP TABLE IF EXISTS flights")
dbExecute(conn, "
  CREATE TABLE flights (
    id INTEGER PRIMARY KEY,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    dep_time INTEGER,
    sched_dep_time INTEGER,
    dep_delay INTEGER,
    arr_time INTEGER,
    sched_arr_time INTEGER,
    arr_delay INTEGER,
    carrier TEXT,
    flight INTEGER,
    tailnum TEXT,
    origin TEXT,
    dest TEXT,
    air_time INTEGER,
    distance INTEGER,
    hour INTEGER,
    minute INTEGER,
    time_hour REAL,
    delayed BOOLEAN
  )
")
dbExecute(conn, "DROP TABLE IF EXISTS avg_delay")
dbExecute(conn, "
  CREATE TABLE avg_delay (
    id INTEGER PRIMARY KEY,
    carrier TEXT,
    avg_dep_delay REAL
  );
")

dbExecute(conn, "DROP TABLE IF EXISTS top_destinations")
dbExecute(conn, "
  CREATE TABLE top_destinations (
  id INTEGER PRIMARY KEY,
   dest TEXT,
   N INTEGER
  );
")

dbWriteTable(conn, "flights", flights_dt, append=TRUE)
dbWriteTable(conn, "avg_delay", avg_delay, append = TRUE)
dbWriteTable(conn, "top_destinations", top_destinations, append = TRUE)
dbDisconnect(conn)






## Perser function
# Credit: https://ambiorix.dev/blog/parse-raw-json/
box::use(
  webutils[parse_http],
)
parse_req <- function(req) {
  tryCatch({
    parse_http(
      body = req$rook.input$read(),
      content_type = req$CONTENT_TYPE
    )
  }, error = function(e) {
    return(NULL)
  })
}



# GET /flight/:id - Get flight details by ID handler function


get_flights<-function(req, res) {
  id <- suppressWarnings(as.integer(req$params$id))
  if (is.na(id) || id <= 0) {
    return(res$set_status(400L)$json(list(error = "Invalid flight ID.")))
  }
  
  # fetch flight from the db
  conn <- dbConnect(SQLite(), "flights.db")
  flight <- dbGetQuery(conn, "SELECT * FROM flights WHERE id = ?", params = list(id))
  
  if (nrow(flight) == 0) {
    dbDisconnect(conn)
    return(res$set_status(404L)$json(list(error = sprintf("No flight found with ID %s", id))))
  }
  dbDisconnect(conn)
  return(res$set_status(200L)$json(flight))
}


## POST Request handler function


post_flight<-function(req, res) {
  content <- tryCatch({
    parsed_content <- parse_req(req)
    if(is.null(parsed_content)){
      return(res$set_status(400L)$json(list(error = " missing request body.")))
    }
    parsed_content=as.data.table(parsed_content)
  }, error = function(e) {
    return(res$set_status(400L)$json(list(error = "Invalid  request body.")))
  })
  
  
  # Drop 'id' if it exists in request body
  if ("id" %in% names(content)) {
    content[, id := NULL]
  }
  
  # Insert into database
  conn <- dbConnect(SQLite(), "flights.db")
  success <- tryCatch({
    dbWriteTable(conn, "flights", content, append = TRUE)
    last_id <- dbGetQuery(conn, "SELECT last_insert_rowid() AS id")$id
    last_id
  }, error = function(e) {
    return(res$set_status(500L)$json(list(error = "Database error. Could not save flight.")))
  })
  
  new_flight <- dbGetQuery(conn, "SELECT * FROM flights WHERE id = ?", params = list(success))
  
  return(res$set_status(201L)$json(list(message = "Flight added successfully", data = new_flight)))
}



##GET /check-delay/:id request handler function
check_delay<- function(req, res) {
  #validating flight id parameter
  id <- suppressWarnings(as.integer(req$params$id))
  if (is.na(id) || id <= 0) {
    return(res$set_status(400L)$json(list(error = "Invalid flight ID.")))
  }
  
  conn <- dbConnect(SQLite(), "flights.db")
  flight <- tryCatch({
    dbGetQuery(conn, "SELECT delayed FROM flights WHERE id = ?", params = list(id))
  }, error = function(e) {
    return(res$set_status(500L)$json(list(error = "Database error occurred.")))
  })
  
  if (nrow(flight) == 0) {
    return(res$set_status(404L)$json(list(error = sprintf("No flight found with ID %s", id))))
  }
  return(res$json(list(delayed = flight$delayed[1])))
}


# GET /avg-dep-delay?id=given-airline-name handler function n
avg_dep_delay<-function(req, res) {
  airline <- req$query$id
  conn <- dbConnect(SQLite(), "flights.db")
  
  # If no airline is provided, return all airlines' average delays
  if (is.null(airline) || airline == "") {
    carrier <- tryCatch({
      dbGetQuery(conn, "SELECT carrier, avg_dep_delay FROM avg_delay")
    }, error = function(e) {
      return(res$set_status(500L)$json(list(error = "Database error occurred.")))
    })
    
    return(res$json(carrier))
  }
  
  
  carrier <- tryCatch({
    dbGetQuery(conn, "SELECT avg_dep_delay FROM avg_delay WHERE carrier = ?", params = list(airline))
  }, error = function(e) {
    return(res$set_status(500L)$json(list(error = "Database error occurred.")))
  })
  if (nrow(carrier) == 0) {
    return(res$set_status(404L)$json(list(error = sprintf("No flight data found for airline %s", airline))))
  }
  return(res$json(carrier))
}



##GET /top-destinations/:n request handler function

top_destinations<-function(req, res) {
  # Validate input 'n' 
  destinations <- tryCatch(as.integer(req$params$n), warning = function(w) NULL, error = function(e) NULL)
  
  if (is.null(destinations) || is.na(destinations) || destinations <= 0) {
    return(res$set_status(400L)$json(list(error = "Invalid destination count. Must be a positive integer.")))
  }
  
  # total available destinations
  conn <- dbConnect(SQLite(), "flights.db")
  all_destinations <- tryCatch({
    dbGetQuery(conn, "SELECT * FROM top_destinations")
  }, error = function(e) {
    return(res$set_status(500L)$json(list(error = "Database error occurred.")))
  })
  
  
  # Check if requested destinations exceed available count
  if (destinations > nrow(all_destinations)) {
    msg <- sprintf("Max top destinations exceeded. Requested: %s, Available: %s", destinations, nrow(all_destinations))
    return(res$set_status(404L)$json(list(error = msg)))
  }
  
  #proceed to query 
  query <- "SELECT * FROM top_destinations LIMIT ?"
  top_destinations <- tryCatch({
    dbGetQuery(conn, query, params = list(destinations))
  }, error = function(e) {
    return(res$set_status(500L)$json(list(error = "Database error occurred while fetching destinations.")))
  })
  
  return(res$set_status(200L)$json(top_destinations))
}


# PUT /flights/:id request handler functio n
modify_flight<-function(req, res) {
  id <- suppressWarnings(as.integer(req$params$id))
  if (is.na(id) || id <= 0) {
    return(res$set_status(400L)$json(list(error = "Invalid flight ID.")))
  }
  
  new_details <- tryCatch({
    parsed_content <- parse_req(req)
    if(is.null(parsed_content)){
      return(res$set_status(400L)$json(list(error = " missing request body.")))
    }
    parsed_content=as.data.table(parsed_content)
  }, error = function(e) {
    return(res$set_status(400L)$json(list(error = "Invalid  request body.")))
  })
  
  # Drop 'id' if it exists in request body
  if ("id" %in% names(new_details)) {
    new_details[, id := NULL]
  }
  
  #get the flight to update
  conn <- dbConnect(SQLite(), "flights.db")
  existing_flight <- dbGetQuery(conn, sprintf("SELECT * FROM flights WHERE id = %d", id))
  if (nrow(existing_flight) == 0) {
    msg <- sprintf("No flight found with id %d", id)
    return(res$set_status(404L)$json(list(error = msg)))
  }
  
  # Update the flight details
  setnames(new_details, names(new_details), paste0("\"", names(new_details), "\""))
  set_query <- paste(sprintf("%s = '%s'", names(new_details), new_details), collapse = ", ")
  update_query <- sprintf("UPDATE flights SET %s WHERE id = %d", set_query, id)
  
  dbExecute(conn, update_query)
  
  # Return updated flight
  updated_flight <- dbGetQuery(conn, sprintf("SELECT * FROM flights WHERE id = %d", id))
  response <- list(
    message = sprintf("Flight with ID %d has been updated successfully.", id),
    new_details = updated_flight
  )
  
  return(res$json(response))
  
}


## DELETE /:id request handler function

delete_flight<-function(req, res) {
  id <- suppressWarnings(as.integer(req$params$id))
  if (is.na(id) || id <= 0) {
    return(res$set_status(400L)$json(list(error = "Invalid flight ID.")))
  }
  
  conn <- dbConnect(SQLite(), "flights.db")
  tryCatch({
    existing_flight <- dbGetQuery(conn, "SELECT * FROM flights WHERE id = ?", params = list(id))
    if (nrow(existing_flight) == 0) {
      return(res$set_status(404L)$json(list(error = sprintf("No flight found with id %d", id))))
    }
    
    # Delete the flight
    dbExecute(conn, "DELETE FROM flights WHERE id = ?", params = list(id))
    msg <- sprintf("Flight with id %d successfully deleted", id)
    res$json(list(message = msg))
  }, error = function(e) {
    res$set_status(500L)$json(list(error = "Internal server error"))
  })
}

Ambiorix$
  new(port=3000)$
  get("/flight/:id", get_flights )$
  post('/flight',post_flight )$
  get("/check-delay/:id", check_delay)$
  get("/avg-dep-delay",avg_dep_delay)$
  get("/top-destinations/:n", top_destinations )$
  put("/flights/:id",modify_flight )$
  delete("/:id",delete_flight )$
  start()
