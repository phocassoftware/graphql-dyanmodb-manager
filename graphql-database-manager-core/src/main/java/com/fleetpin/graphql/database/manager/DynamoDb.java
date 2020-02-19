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

import com.fleetpin.graphql.database.manager.table.Table;
import org.dataloader.DataLoader;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public abstract class DynamoDb {
	public abstract <T extends Table> CompletableFuture<T> delete(String organisationId, T entity);

	public abstract  <T extends Table> CompletableFuture<T> deleteLinks(String organisationId, T entity);

	public abstract <T extends Table> CompletableFuture<T> put(String organisationId, T entity);

	public abstract CompletableFuture<List<DynamoItem>> get(List<DatabaseKey> keys);

	public abstract CompletableFuture<List<DynamoItem>> getViaLinks(String organisationId, Table entry, Class<? extends Table> type, DataLoader<DatabaseKey, DynamoItem> items);

	public abstract CompletableFuture<List<DynamoItem>> query(DatabaseQueryKey key);

	public abstract CompletableFuture<List<DynamoItem>> queryGlobal(Class<? extends Table> type, String value);

	public abstract CompletableFuture<List<DynamoItem>> querySecondary(Class<? extends Table> type, String organisationId, String value);

	public abstract <T extends Table> CompletableFuture<T> link(String organisationId, T entry, Class<? extends Table> class1, List<String> groupIds);

    protected int maxBatchSize();

    public abstract String newId();
    
    protected String getSourceTable(Table table) {
		return table.getSourceTable();
	}
}
