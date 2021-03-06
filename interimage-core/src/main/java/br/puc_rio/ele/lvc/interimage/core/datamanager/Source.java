<<<<<<< HEAD
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

import java.io.File;

/**
 * An interface for sources. 
 * @author Rodrigo Ferreira
 */
public interface Source {

	public void put(String from, String to, Resource resource);
	
	public void multiplePut(File dir, String key);
	
	public void makePublic(String key);
	
	public void close();
	
	public String getURL();
	
	public String getSpecificURL();
	
}
=======
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

import java.io.File;

/**
 * An interface for sources. 
 * @author Rodrigo Ferreira
 */
public interface Source {

	public void put(String from, String to, Resource resource);
	
	public void multiplePut(File dir, String key);
	
	public void makePublic(String key);
	
	public void close();
	
	public String getURL();
	
	public String getSpecificURL();
	
}
>>>>>>> branch 'master' of https://github.com/darioaugusto/interimage-3
