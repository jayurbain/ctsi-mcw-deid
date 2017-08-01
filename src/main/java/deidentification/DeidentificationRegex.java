/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
    This file is part of "CTSI MCW NLP" for removing
    protected health information from medical records.

    "CTSI MCW NLP" is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    "CTSI MCW NLP" is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with "CTSI MCW NLP."  If not, see <http://www.gnu.org/licenses/>.
 */
/**
 * @author jayurbain, jay.urbain@gmail.com
 * 
 */
package deidentification;

import java.util.List;
import java.util.Map;


/**
 * @author jayurbain
 *
 * Interface for de-identification regular expressions
 * Where to handle MRN, dates with dateOffset, email, phone, address
 */
public interface DeidentificationRegex {

	/**
	 * @param String - s, text to convert
	 * @param String - dateStr, date of service
	 * @param String - dateOffset, day offset assigned to this patient for date conversion
	 * 
	 * Place preprocessing regular expression conversions method calls here
	 * Called from deidentification process.
	 * 
	 * Note: Order dependent!
	 * 
	 */

	public String compositeRegex(String s, int dateOffset, Map<String, String> blackListMap, String patientName );

}

