/*
 * Copyright 2017 MapD Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * File:   ParserWrapper.cpp
 * Author: michael
 *
 * Created on Feb 23, 2016, 9:33 AM
 */

#include "ParserWrapper.h"
#include "Shared/measure.h"

#include <boost/algorithm/string.hpp>
#include <boost/tokenizer.hpp>

#include <glog/logging.h>

using namespace std;

const std::vector<std::string> ParserWrapper::ddl_cmd =
    {"ALTER", "COPY", "GRANT", "CREATE", "DROP", "REVOKE", "SHOW", "TRUNCATE"};

const std::vector<std::string> ParserWrapper::update_dml_cmd = {
    "INSERT",
    "DELETE",
    "UPDATE",
    "UPSERT",
};

const std::string ParserWrapper::explain_str = {"explain"};
const std::string ParserWrapper::calcite_explain_str = {"explain calcite"};

ParserWrapper::ParserWrapper(std::string query_string) {
  if (boost::istarts_with(query_string, calcite_explain_str)) {
    actual_query = boost::trim_copy(query_string.substr(calcite_explain_str.size()));
    ParserWrapper inner{actual_query};
    if (inner.is_ddl || inner.is_update_dml) {
      is_other_explain = true;
      return;
    } else if (inner.is_sqlplus_cmd) {
      // EXPLAIN sql DESC command, just execute DESC command
      is_sqlplus_cmd = inner.is_sqlplus_cmd;
      is_describer = inner.is_describer;
      table_name = inner.table_name;
      is_other_explain = false;
    } else {
      is_select_calcite_explain = true;
      return;
    }
  }

  if (boost::istarts_with(query_string, explain_str)) {
    actual_query = boost::trim_copy(query_string.substr(explain_str.size()));
    ParserWrapper inner{actual_query};
    if (inner.is_ddl || inner.is_update_dml) {
      is_other_explain = true;
      return;
    } else if (inner.is_sqlplus_cmd) {
      // EXPLAIN sql DESC command, just execute DESC command
      is_sqlplus_cmd = inner.is_sqlplus_cmd;
      is_describer = inner.is_describer;
      table_name = inner.table_name;
      is_other_explain = false;
    } else {
      is_select_explain = true;
      return;
    }
  }

  for (std::string ddl : ddl_cmd) {
    is_ddl = boost::istarts_with(query_string, ddl);
    if (is_ddl) {
      if (ddl == "COPY") {
        is_copy = true;
        // now check if it is COPY TO
        boost::regex copy_to{R"(COPY\s*\(([^#])(.+)\)\s+TO\s)", boost::regex::extended | boost::regex::icase};
        if (boost::regex_match(query_string, copy_to)) {
          is_copy_to = true;
        }
      }
      return;
    }
  }

  for (int i = 0; i < update_dml_cmd.size(); i++) {
    is_update_dml = boost::istarts_with(query_string, ParserWrapper::update_dml_cmd[i]);
    if (is_update_dml) {
      dml_type = (DMLType)(i);
      return;
    }
  }

  // Check if it is SQL plus commands, DESCRIBER

  const char* delim = "\t ";
  boost::char_separator<char> sep{delim, "", boost::drop_empty_tokens};
  typedef boost::tokenizer<boost::char_separator<char>> tokenizer;

  actual_query = boost::trim_copy_if(query_string, boost::algorithm::is_any_of("; \t"));
  tokenizer tok{actual_query, sep};

  tokenizer::iterator it=tok.begin();
  if (*it == "DESC" || *it == "DESCRIBER" || *it == "desc" || *it == "describer") {
    it++;
    if (it == tok.end()) return;

    table_name = *it;

    it++;
    if (it != tok.end()) return;

    is_describer = true;
    is_sqlplus_cmd = true;
    return;
  }
}

ParserWrapper::~ParserWrapper() {}
