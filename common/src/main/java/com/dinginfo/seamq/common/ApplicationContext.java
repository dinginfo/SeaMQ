package com.dinginfo.seamq.common;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class ApplicationContext {
	private final Map<Key<?>, Object> objMap = new HashMap<Key<?>, Object>();

	public <T> void putBean(String id, Object obj) {
		if (id == null || obj == null) {
			return;
		}
		Key<T> key = new Key(id, obj.getClass());
		objMap.put(key, obj);
	}

	public <T> T getBean(String id, Class<T> classType) {
		Key<T> key = new Key<T>(id, classType);
		Object obj = objMap.get(key);
		if (obj != null) {
			return classType.cast(obj);
		} else {
			return null;
		}
	}

	class Key<T> implements Serializable {
		private String id;

		private Class<T> type;

		public Key(String id, Class<T> type) {
			this.id = id;
			this.type = type;
		}

		public Class<T> getType() {
			return type;
		}

		public String getId() {
			return id;
		}

		@Override
		public int hashCode() {
			if (id != null) {
				return id.hashCode();
			} else {
				return 0;
			}
		}

		@Override
		public boolean equals(Object obj) {
			if (id == null || obj == null) {
				return false;
			} else {
				Key object = (Key) obj;
				return id.equals(object.getId());
			}
		}
	}
}
