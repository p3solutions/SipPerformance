/*
========================================================================
SchemaCrawler
http://www.schemacrawler.com
Copyright (c) 2000-2016, Sualeh Fatehi <sualeh@hotmail.com>.
All rights reserved.
------------------------------------------------------------------------

SchemaCrawler is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

SchemaCrawler and the accompanying materials are made available under
the terms of the Eclipse Public License v1.0, GNU General Public License
v3 or GNU Lesser General Public License v3.

You may elect to redistribute this code under any of these licenses.

The Eclipse Public License is available at:
http://www.eclipse.org/legal/epl-v10.html

The GNU General Public License v3 and the GNU Lesser General Public
License v3 are available at:
http://www.gnu.org/licenses/

========================================================================
*/
package com.p3.archon.sip_process.bean;

import java.sql.Blob;

/**
 * Created by Suriyanarayanan K
 * on 08/05/20 3:17 PM.
 */
public class BinaryData {

	private final boolean hasData;
	private final String data;
	private final Blob blob;

	public BinaryData() {
		data = null;
		hasData = false;
		blob = null;
	}

	public BinaryData(final String data, Blob blob) {
		this.blob = blob;
		this.data = data;
		hasData = true;
	}

	public Blob getBlob() {
		return blob;
	}

	public boolean hasData() {
		return hasData;
	}

	@Override
	public String toString() {
		if (hasData) {
			return data;
		} else {
			return "<binary>";
		}
	}

}
