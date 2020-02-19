/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.fleetpin.graphql.database.manager;

import java.util.function.Supplier;

import com.fleetpin.graphql.database.manager.access.ModificationPermission;

public abstract class DatabaseManager {
	private final Supplier<String> idGenerator;
	
	private DatabaseManager(Supplier<String> idGenerator) {
		this.idGenerator = idGenerator;
	}
	
	public abstract Database getDatabase(String organisationId);
	
	
	public abstract Database getDatabase(String organisationId, ModificationPermission putAllow);
	
	public String newId() {
		return idGenerator.get();
	}
}