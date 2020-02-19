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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import com.fleetpin.graphql.database.manager.annotations.GlobalIndex;
import com.fleetpin.graphql.database.manager.annotations.SecondaryIndex;
import com.fleetpin.graphql.database.manager.annotations.TableName;

public class TableUtil {

	static String getSecondaryGlobal(Table entity) {
		for (var method : entity.getClass().getMethods()) {
			if (method.isAnnotationPresent(GlobalIndex.class)) {
				try {
					var secondary = method.invoke(entity);
					if (secondary instanceof Optional) {
						secondary = ((Optional) secondary).orElse(null);
					}
					return (String) secondary;
				} catch (ReflectiveOperationException e) {
					throw new RuntimeException(e);
				}
			}
		}
		return null;
	}

	static String getSecondaryOrganisation(Table entity) {
		for (var method : entity.getClass().getMethods()) {
			if (method.isAnnotationPresent(SecondaryIndex.class)) {
				try {
					var secondary = method.invoke(entity);
					if (secondary instanceof Optional) {
						secondary = ((Optional) secondary).orElse(null);
					}
					return (String) secondary;
				} catch (ReflectiveOperationException e) {
					throw new RuntimeException(e);
				}
			}
		}
		return null;
	}

	static <T> CompletableFuture<List<T>> all(List<CompletableFuture<T>> collect) {
		return CompletableFuture.allOf(collect.toArray(CompletableFuture[]::new))
				.thenApply(__ -> collect.stream().map(m -> {
					try {
						return m.get();
					} catch (InterruptedException | ExecutionException e) {
						throw new RuntimeException(e);
					}
				}).collect(Collectors.toList()));
	}
	//shold we hide this logic or leave it visible?
	public static String table(Class<? extends Table> type) {
		Class<?> tmp = type;
		TableName name = null;
		while (name == null && tmp != null) {
			name = tmp.getDeclaredAnnotation(TableName.class);
			tmp = tmp.getSuperclass();
		}
		if (name == null) {
			return type.getSimpleName().toLowerCase() + "s";
		} else {
			return name.value();
		}
	}
}
