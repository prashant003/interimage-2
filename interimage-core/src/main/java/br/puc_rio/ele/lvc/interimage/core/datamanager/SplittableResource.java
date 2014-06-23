/*Copyright 2014 Computer Vision Lab

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.*/

package br.puc_rio.ele.lvc.interimage.core.datamanager;

/**
 * A class that defines a splittable resource. 
 * @author Rodrigo Ferreira
 */
public class SplittableResource implements Resource {
	
	public static final short IMAGE = 1;
	public static final short SHAPE = 2;
	
	private Object _object;
	private short _type;	
	
	public SplittableResource(Object obj, short type) {
		_object = obj;
		_type = type;
	}
	
	public short getType() {
		return _type;
	}
	
	public Object getObject() {
		return _object;
	}
	
}
