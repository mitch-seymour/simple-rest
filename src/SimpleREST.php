<?php
/**
 * Simple REST Client
 *
 * This class is a very simple and lightweight REST client that can
 * be used for making basic DELETE/GET/POST/PUT http requests. It has
 * generalized methods that can work with many APIs, including HBase,
 * Lync, etc.
 *
 * Usage:
 * ------ 
 * $rest = new SimpleREST('http://requestb.in'); // provide the base url
 *
 * // Formats
 * $rest->binary();
 * $rest->html();
 * $rest->json();
 * $rest->plain();
 * $rest->protobuf();
 * $rest->xml();
 *
 * // Responses are converted into arrays, and base64_decode is called
 * $rest->autoConvert(true);
 *
 * $requestData = array('name' => 'Mitch', 'age' => 28);
 * $rest->location('relative/path/to/url')->delete();
 * $rest->location('relative/path/to/url')->get($requestData);
 * $rest->location('relative/path/to/url')->post($requestData);
 * $rest->location('relative/path/to/url')->put();
 *
 * // Sometimes, you may not need to to wait for a response
 * $rest->wait(false);
 *
 * // Setting headers
 * $rest->headers(array('Referer' => 'https://google.com')); 
 *
 * // Get info about the last request
 * $infoArr = $rest->lastRequest();
 * $resCode = $rest->responseCode();
 *
 * // follow redicts
 * $rest->follow(true);
 *
 * HBase Examples (Using the Stargate API)
 * ---------------------------------------
 * $clusterStatus = $rest->location('status/cluster')->get();
 * $regions = $rest->location('MitchTest/regions')->get();
 * $schema = $rest->location('MitchTest/schema')->get();
 * $tables = $rest->location()->get();
 * $version = $rest->location('version')->get();
 * $versionCluster = $rest->location('version/cluster')->get();
 *
 * HBase CRUD Queries (Stargate)
 * -----------------------------
 * $rows = $rest->location('MitchTest/row1')->get();
 * $rows = $rest->location('MitchTest/*ASPECTJ*')->get(); // Filter row keys
 * $rows = $rest->location('MitchTest/*ASPECTJ*')->get(array('startrow' => '*', 'limit' => 30)); // Limit clause
 * // More coming soon...
 *
 * @author <mitchseymour@gmail.com>
 * @version 0.0.1
 */

namespace Simpler\Datastore;

/**
 * Interface for a Simple Data Store
 *
 * @version 0.0.1
 */
interface SimpleDataStore {

	/**
	 * Client Type
	 *
	 * The type of data store that the child class is implementing. This is used
	 * for caching handles, and retrieving settings from the main configuration
	 * file (etc/settings.json)
	 *
	 * @return string $clientType
	 */
	static function clientType();
	
	/**
	 * Configurable Query Types
	 *
	 * This method should return an array of configurable query types.
	 *
	 * @return array $types
	 */
	static function configurableQueryTypes();
	
	/**
	 * Get handle from settings
	 *
	 * Since each client class (i.e. data store) will have different methods
	 * for generating a handle, child classes must implement this method
	 * to handle the process of generating a handle
	 *
	 * @return mixes $handle
	 */
	function getHandleFromSettings($params);

}

/**
 * Abstract DataStore class
 *
 * @version 0.0.1
 */
abstract class DataStore implements SimpleDataStore {
	
	/**
	 * @var boolean $adHoc
	 *
	 * Whether or not the child object can be instantiated
	 * without any predefined settings (usually in the settings.json
	 * file)
	 */
	protected $adHoc = false;
	/**
	 * @var bool static $debug
	 *
	 * Whether or not debug mode is activated.
	 */
	private static $debug;
	
	/**
	 * @var string $environment
	 * 
	 * The current environment that the client is running in. Note: this
	 * can be overridden using the self::simulateEnvironment method.
	 */
	protected $environment;
	
	/**
	 * @var array $hooks
	 * 
	 * Hooks that have been configured at the ** instance ** level for peforming
	 * client-defined actions during certain points in the data store's
	 * execution
	 */
	protected $hooks = array();
	
	/**
	 * @var array $_hooks
	 * 
	 * Hooks that have been configured at the ** class ** level for peforming
	 * client-defined actions during certain points in the data store's
	 * execution
	 */
	protected static $_hooks = array();
	
	/**
	 * @var string $name
	 *
	 * The client identifier
	 */
	protected $name;
	
	/**
	 * @var bool static $initialized
	 *
	 * Whether or not this client has been initialized. Since multiple clients
	 * can be invoked in the same process, we wrap all of the initialization
	 * code in a single, static constructor: self::init()
	 */
	private static $initialized;
	
	/**
	 * @var array static $handles
	 *
	 * An array of cached handles
	 */
	private static $handles;
	
	/**
	 * @var array $overrideSettings
	 *
	 * An array of settings that were passed when the object was instantiated
	 */
	protected $overrideSettings = array();
	
	/**
	 * @var static array $settings
	 *
	 * Holds the settings for each unique client type that is instantiated
	 */
	private static $settings;
		
	/**
	 * Constructor
	 *
	 * Note: initialization is delegated to a static method (self::init)
	 * to prevent multiple clients from performing the same basic initialization
	 * tasks
	 *
	 * @return void
	 */
	public function __construct($name='localhost', $environment='Development'){
		
		if (is_array($name)){
			$this->overrideSettings = $name;
			return;
		}
		
        // Set the name / base URI
		$this->name = $name;
		
		// Set the environment
		$this->environment = $environment;
		
		// This initialization procedure will only be called once for each type of client
		if (!self::$initialized[static::clientType()])
			$this->init();
	}
	
	/**
	 * Save an array as a CSV file
	 *
	 * @param string $file - The name of the file to put the CSV in
	 * @param array $arr - The array to convert to a CSV list
	 * @param optional string $sep - The field separator
	 *
	 * @return string $csv - The CSV version of the array
	 */
	public static function array2CSV($file, array $arr, $sep=',', $keys=false){
		
		$totalWritten = 0;
		
		if (is_resource($file)){
			
			$fh = $file;
		
		} else if (!$fh = fopen($file, 'w')){
		
			throw new Exception('Could not open file for writing: ' . $file);
		}
		
		if ($keys){
			$totalWritten += fputcsv($fh, array_keys($arr[0]), $sep, '"');
		}
		
		foreach ($arr as $fields)
			$totalWritten += fputcsv($fh, $fields, $sep, '"');
		
		
		if (!is_resource($file)){
			
			fclose($fh);
			return file_get_contents($file);
		}
		
		return $totalWritten;
	}	
	
	/**
	 * Before Handle Returned
	 *
	 * Child classes have the option of implementing this method, which gives
	 * them greater flexibility in managing the settings/handle parameters
	 * whenever a handle is retrieved.
	 *
	 * @return void
	 */
	protected function beforeHandleReturned($handle){
		
		if ($_handle = $this->callHook('beforeHandleReturned', $handle))
			return $_handle;
			
		return $handle;
	}
	
	/**
	 * Call hook
	 *
	 * This method can be used by child classes to insert hooks during certain points
	 * of the program's execution. For example, a Data Store that runs
	 * queries against a database may use: $this->callHook('beforeQuery', $sql)
	 * before a query is actually run, which allows clients to execute their own functions
	 *  by registering a hook with: $dataStore->hook('beforeQuery', function(){});
	 *
	 * @param mixed $params - Any number of parameters, with the first parameter always being the
							  hook name, and the rest of the parameters being passed directly
							  to the registered hook function
	 *
	 * @return mixed $results - The results of the client function if defined, otherwise false
	 */
	public function callHook(){
		
		$args = func_get_args();
		
		if (count($args) < 1)
			return false;
			
		$hook = array_shift($args);
		
		$clientType = static::clientType();
		
		if (isset($this->hooks[$clientType][$hook])){
			
			// a hook was cnofigured at the instance level
			self::__log("Calling instance-level hook: {$hook}");
			return call_user_func_array($this->hooks[$clientType][$hook], $args);
		
		} else if (isset(self::$_hooks[$clientType][$hook])){
			
			// a hook was cnofigured at the class level
			self::__log("Calling class-level hook: {$hook}");
			return call_user_func_array(self::$_hooks[$clientType][$hook], $args);
			
		}
		
		return false;
	
	}
	
	/**
	 * Get Temporary File
	 *
	 * @param optional string $prefix - The prefix to be used in the file name
	 * @param optional string $extension - The extenstion of the file (defaults to csv)
	 *
	 * @return string $fileName - The name of a temporary file
	 */
	public function getTemporaryFile($prefix='temp_datastore_', $extension ='.csv'){
		
		return self::temporaryFile($prefix, $extension);
	
	}
	
	public static function temporaryFile($prefix='temp_datastore_', $extension ='.csv'){
		
		return 'tmp/' .  uniqid($prefix, true) . $extension;
		
	}
	
	/**
	 * Delete a file
	 *
	 * @param mixed $fileName - Either a file handle, or a string representing the file name to be deleted
	 * @return svoid
	 */
	public static function deleteFile($fileName){
		
		if (is_resource($fileName)){
			
			// user provided a resource, so we need to close the file first
			$_file = stream_get_meta_data($fileName)['uri'];
			fclose($fileName);
			//unlink($_file);
		
		} else {
		
			unlink($_file);
		}
	
	}
	
	/**
	 * Hook
	 *
	 * Register a hook at the instance level
	 *
	 * @param string $name - The name of the hook to register
	 * @param callable $fn - The function to execute when the hook event occurs
	 * @return DataStore $this - The current object. This allows additional methods to be chained
	 */
	public function hook($name, $fn){
	
		$this->hooks[static::clientType()][$name] = $fn;
		return $this;
	}
	
	/**
	 * Global hook
	 *
	 * Register a hook at the class level
	 *
	 * @param string $name - The name of the hook to register
	 * @param callable $fn - The function to execute when the hook event occurs
	 * @return DataStore $this - The current object. This allows additional methods to be chained
	 */
	public static function globalHook($name, $fn){
		
		self::$_hooks[static::clientType()][$name] = $fn;
	
	}
	
	/**
	 * Get handle
	 *
	 * This method determines whether or not a handle has been cached for
	 * the current client + query type, and if not, delegates to the 
	 * child's `getHandleFromSettings` method for retrieving the handle/
	 *
	 * @param string $type   - The query type to be used on the handle	 
	 * @return mixed $handle - Usually, an array containing the handle.
	 *						   However, clients can override the 
	 *						   `beforeHandleReturned` method to return
	 *						   a different data type (e.g. a resource)
	 */
	public function getHandle($type){
		
		// Check to see if the settings were provided during instantiation
		if ($this->overrideSettings){
			$handle = $this->getHandleFromSettings($this->overrideSettings);
			return $this->beforeHandleReturned($handle); 
		}
		
		
		$settings   = $this->getSettings($this->name);
		$clientType = static::clientType();
		
		if (!$settings){
			
			if ($this->adHoc){
				$handle = $this->getHandleFromSettings(array());
				return $this->beforeHandleReturned($handle); 
			}

			return false;
		}

		// If the client is executing a configurable query type, get a handle that allows it
		if (in_array($type, $this->configurableQueryTypes())){
			
			
			// See if a reference to this object already exists
			if (isset(self::$handles[$clientType][$this->name][$type])){
				
				return $this->beforeHandleReturned(self::$handles[$clientType][$this->name][$type]);
			}
			
			// No reference exists, so we need to create them
			foreach ($settings as $index => $params){
				
				if (isset($params['allow'][$type])){
					
					$handle = $this->getHandleFromSettings($params);
					
					$this->saveHandle($handle, array_flip($params['allow']));
					return $this->beforeHandleReturned($handle);
	
				}
			}
			
		} else {
			
			// See if a reference to this object already exists
			if (isset(self::$handles[$clientType][$this->name]['*'])){

				return $this->beforeHandleReturned(self::$handles[$clientType][$this->name]['*']);
			}
			
			// The client is not executing a configurable query type
			$params = $settings[0];
			$handle = $this->getHandleFromSettings($params);
			$this->saveHandle($handle, array_flip($params['allow']));
			return $this->beforeHandleReturned($handle);

		}

		
	}
	
	/**
	 * Save handle
	 *
	 * This method caches handles that can retrieved at later times
	 * during the execution of a script
	 *
	 * @param mixed $handle - The handle to be cached
	 * @param array $queryTypes - The types of queries for which the
	 *							  cached handle can be used
	 * @return void
	 */
	private function saveHandle($handle, array $queryTypes){
		
		$clientType = static::clientType();
		
		// add references to this object
		foreach ($queryTypes as $allowType)
			self::$handles[$clientType][$this->name][$allowType] = $handle;
	}
			
	/**
	 * Debug
	 *
	 * Turns debug mode on and off
	 *
	 * @params optional boolean - Whether or not the client wants debug output
	 *							  echoed to the  output device
	 * @return void
	 */
	public static function debug($bool=true){
	
		self::$debug = (bool) $bool;
	}
	
	/**
	 * Init
	 *
	 * Shared initialization tasks are handled by this method.
	 *
	 * @params string $type - The type of client. Usually the top level key
	 *						  for the client settings located in etc/settings.json
	 *						  (examples: "Databases", "REST Clients", etc)
	 * @return void
	 */
	private function init(){
		
		$environment = $this->environment;
		$settings    = array();
		$type        = static::clientType();
		
		// Get the configurable query types
		$configurableQueryTypes = $this->configurableQueryTypes();
		
		if (!is_array($configurableQueryTypes))
			$configurableQueryTypes = array_map('trim', explode(',', $configurableQueryTypes));
		
		self::__log("Initializing DataStore client ({$type}) for the following environment: {$environment}");
	
	}
	
	/**
	 * Log messages
	 *
	 * This method is used for logging messages. This is a crude implementation,
	 * and is only meant for debugging in a dev environment!
	 *
	 * @return void
	 */
	protected static function __log($message){
		
		if (self::$debug){
			echo $message , PHP_EOL;
		}
	
	}
	
	/**
	 * Get settings
	 *
	 * Retrieves the settings for the current client + client name
	 *
	 * @return mixed $settings - An array of settings if found, otherwise false
	 */
	protected function getSettings(){
	
		if (isset(self::$settings[static::clientType()][$this->name]))
			return self::$settings[static::clientType()][$this->name];
			
		return false;
		
	}
	
	/**
	 * Re-index array
	 *
	 * This method will re-index arrays up to 2 nested values
	 *
	 * @params array $arr     - The array to be re-indexed
	 * @params string $column - The name of the key whose value is to be used
	 *							as the new index
	 
	 * @params optional boolean $unset - Whether or not the values that are being moved
	 *									 to the new indexes should be unset
	 *
	 * @return array $arr - The newly re-indexed array
	 */
	public function reindex($arr, $column, $unset=true){
		
		$reindexed = array();
	
		if (is_array($arr)){
			
			if (is_array($column) && count($column) == 2) {
			
				$k1 = $column[0];
				$k2 = $column[1];
				
				foreach ($arr as $row) {
					$key1 = $row[$k1];
					$key2 = $row[$k2];
					$reindexed[$key1][$key2] = $row;		
				}
			
			} else {		
			
				foreach ($arr as $row) {
					
					$key = $row[$column];
					
					if ($unset)
						unset($row[$column]);
						
					$reindexed[$key][] = $row;
					
				}	
			}	
		}
		
		return $reindexed;
	
	}
	
}

class SimpleREST extends DataStore {

	/**
	 * @var boolean $adHoc
	 * 
	 * Allows users to create instances of this class without
	 * configuring a REST client in settings.json
	 */
	protected $adHoc = true;
	
	/**
	 * @var boolean $async
	 * 
	 * Note: Async is currently not implemented! This functionality
	 * will be added at a later time
	 *
	 * Whether or not requests should be executed asynchronously
	 */
	protected $async = false;

	/**
	 * @var boolean $autoConvert
	 * 
	 * Whether or not the responses should be automatically converted
	 * into arrays (JSON) or XML docs (XML)
	 */
	private $autoConvert;
	
	/**
	 * @var boolean $autoSetCookies
	 * 
	 * Whether or not cookies should automatically be set
	 * if Set-Cookie is specified in the response 
	 */
	private $autoSetCookies = false;
	
	/**
	 * @var string $acceptHeader
	 * 
	 * The expected encoding of responses (e.g. json, xml, binary, protobufs)
	 */
	private $acceptHeader;	
	
	/**
	 * @var string $contentType
	 * 
	 * The value of the Content-Type header
	 */
	private $contentType;
	
	/**
	 * @var array $cookies
	 * 
	 * An array of cookies to be included with the request header
	 */
	private $cookies = array();
	
	/**
	 * @var array $cookieHistory
	 * 
	 * An array containing a list of cookies set so far
	 */
	private $cookieHistory = array();
	
	/**
	 * @var boolean $follow
	 * 
	 * Whether or not redirects should be followed
	 */
	private $follow = false;
	
	/**
	 * @var array $headersSent
	 * 
	 * The headers that were sent in the last request
	 */
	private $headersSent;	

	/**
	 * @var array $lastInfo
	 * 
	 * Information regarding the last request/transfer
	 */
	private $lastInfo = array();
	
	/**
	 * @var string $lastUrl
	 * 
	 * The last url that was set using the chainable 
	 * self::$location method
	 */
	private $lastUrl = '';
	
	/**
	 * @var string $lastHeaders
	 * 
	 * The last set of headers that was set using the chainable 
	 * self::$headers method
	 */
	private $lastHeaders = array();
	
	/**
	 * @var boolean $wait
	 * 
	 * Whether or not the client should wait for responses before
	 * continuing execution
	 */
	protected $wait = true;
	
	/**
	 * @var string $xmlVersion
	 * 
	 * The xml version to be used when auto-converting XML responses
	 * to DOMDocuments
	 */
	private $xmlVersion;
	
	/**
	 * Accepts
	 *
	 * Determines whether or not the provided content type is accepted by looking
	 * at the accept header.
	 *
	 * @param string $contentType - the content type to examine
	 * @return boolean $bool - true if the $contentType is accepted, otherwise false
	 */
	private function accepts($contentType){
	
		if ($this->acceptHeader && strpos(strtolower($this->acceptHeader), strtolower($contentType)) !== false)
			return true;
			
		return false;
			
	}
	
	/**
	 * Add Cookie
	 *
	 * Adds a cookie to the request header
	 *
	 * @param string $key - The cookie name
	 * @param optional string $val - The cookie value
	 * @return void
	 */
	public function addCookie($key, $val=null){
	
		if ($val == null && strpos($key, '=') !== false){
			list($key, $val) = explode('=', $key, 2);
		}
		
		$this->headers['Cookie'] = $key . '=' . $val;
		
		//$this->cookies[$key] = $val;
	
	}
	
	/**
	 * Remove Cookie
	 *
	 * Removes a cookie from the request header
	 *
	 * @param string $key - The name of the cookie to be removed
	 * @return void
	 */
	public function removeCookie($key){
	
		if (isset($this->cookies[$key]))
			unset($this->cookies[$key]);
	}
	
	/**
	 * Async
	 *
	 * @param optional boolean $bool - true if requests should be submitted asynchronously,
	 *						  otherwise, false
	 * @return $this
	 */
	public function async($bool=true){
	
		$this->async = (boolean) $bool;
		return $this;
	}
	
	/**
	 * Async
	 *
	 * @param optional boolean $bool - true if requests should be submitted asynchronously,
	 *						  otherwise, false
	 * @return $this
	 */
	public function sync($bool=true){
	
		$this->async = ! (boolean) $bool;
		return $this;
	}
	
	/**
	 * Wait
	 *
	 * @param optional boolean $bool - true if the client should wait for responses.
	 * @return $this
	 */
	public function wait($bool=true){
	
		$this->wait = (boolean) $bool;
		return $this;
	}
	
	/**
	 * isJSON
	 *
	 * @param string $str - The string to be evaluated
	 * @return boolean $isJSON - Whether or not the string provided is valid JSON
	 */
	public function isJSON($str){
		json_decode($str);
		return (json_last_error() == JSON_ERROR_NONE);
	}
	
	/**
	 * Auto Convert
	 *
	 * @param boolean $bool - true if response should be automtically converted
	 * @return $this
	 */
	public function autoConvert($bool=true){
	
		$this->autoConvert = (boolean) $bool;
		return $this;
	}
		
	/**
	 * Before handle returned (overrides parent method)
	 *
	 * This method is called before any handle is returned.
	 * It allows us to configured the curl settings based on
	 * some previous methods that were called before the handle
	 * was retrieved
	 *
	 * @return string $clientType - The client type for this DataStore
	 */
	protected function beforeHandleReturned($c){
	
		if ($this->lastUrl){
			
			if (strpos($this->lastUrl, '://') !== FALSE)
				$c['address'] = $this->lastUrl;
			else
				$c['address'] = $c['address'] . '/'  . $this->lastUrl;
				
			$this->lastUrl = '';
			
		}
		if ($this->acceptHeader)
			$c['headers'][] = 'Accept: ' . $this->acceptHeader;
		
		if ($this->contentType)
			$c['headers'][] = 'Content-Type: ' . $this->contentType;
		
		if ($this->lastHeaders){
			
			$c['headers'] = array_merge($c['headers'], $this->lastHeaders);
		}
		
		
		$this->headersSent = $c['headers'];
		curl_setopt($c['handle'], CURLOPT_URL, $c['address']);
		curl_setopt($c['handle'], CURLOPT_PUT, 0);
		curl_setopt($c['handle'], CURLOPT_HTTPHEADER, array_unique($c['headers']));
		curl_setopt($c['handle'], CURLOPT_FOLLOWLOCATION, $this->follow);
		
		return $c;
	}

	/**
	 * Client type (abstracted in parent class)
	 *
	 * @return string $clientType - The client type for this DataStore
	 */
	public static function clientType(){
	
		return 'REST Clients';
	}
	
	/**
	 * Configurable query types (abstracted in parent class)
	 *
	 * Different handles can be retrieved depending on the query type. To
	 * implement this functionality, you must create an "allow" parameter
	 * for this clientType in the settings.json file, that includes an
	 * array of query types (see below) that can be performed on the 
	 * selected handle
	 *
	 * @return array $queryTypes
	 */
	public static function configurableQueryTypes(){
	
		return array('delete', 'get', 'post', 'put');
	}
	
	/**
	 * Get handle from settings (abstracted in parent class)
	 *
	 * This method obtains a curl handle for performing HTTP requests
	 *
	 * @param array $params - The parameters configured for this handle type
	 * 						  in the settings.json file
	 *
	 * @return array $settings - An array of settings, which holds the both curl handle
	 *							 handle and additional parameters defined in settings.json
	 */
	public function getHandleFromSettings($params){
	
		$ch = curl_init();
		
		// default settings
        $defaults = array(
            'address' => $this->name,
            'user' => false, 
            'pass' => false, 
            'userAgent' => false, 
            'headers' => array()
        );
        
        // merge the defaults with the client's parameters
		$settings = array_merge($defaults, $params);
		
		// Append the credentials to the url if a user and pass were specified	
		if ($settings['user'] && $settings['pass']){
		
			$address = $settings['address'];
			$creds = $settings['user'] . ':' . $settings['pass'];
			
			list($protocol, $addr) = explode('://', $address);
			$settings['address'] = "{$protocol}://{$creds}@{$addr}";
		}
	
		if (!is_array($settings['headers']))
			$settings['headers'] = array_map('trim', explode(',', $settings['headers']));
		
		curl_setopt($ch, CURLOPT_URL, $settings['address']);
		curl_setopt($ch, CURLOPT_HEADER, true);
		curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
		curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, false);
		curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
		

		if ($settings['userAgent'])
			curl_setopt($ch, CURLOPT_USERAGENT, $settings['userAgent']);
			curl_setopt($ch, CURLOPT_USERAGENT, $settings['userAgent']);

		$settings['handle'] = $ch;

		return $settings;
	}
	
	/**
	 * Extract Headers
	 *
	 * Converts a header string to an array
	 *
	 * @return $this
	 */
	private function extractHeaders($headerStr){
		
		$responseHeaders = array_filter(explode("\n", $headerStr));
		$headers = array();
		
		foreach ($responseHeaders as $str){
			
			if (strpos($str, ':') !== false){
			
				list($header, $value) = array_map('trim', explode(':', $str, 2));
				$headers[$header] = $value;
			
			}
		}
		
		return $headers;
		
	}
	
	/**
	 * Follow redirects
	 *
	 * @return $this
	 */
	public function follow($follow=true){
		
		$this->follow = $follow;
		return $this;
		
	}
	
	/**
	 * Location
	 *
	 * Sets the location for the next HTTP request. This method is chainable
	 *
	 * @return $this
	 */
	public function location($url=''){
		
		$this->lastUrl = $url;
		return $this;
	}
	
	/**
	 * Headers
	 *
	 * Sets the headers for the next HTTP request. This method is chainable
	 *
	 * @return $this
	 */
	public function headers(array $headers=array(), $merge=false){
		
		$h = array();
		
		foreach ($headers as $key => $val)
			$h[] = $key . ": " . $val;
		
		if ($merge)
			$this->lastHeaders = array_merge($this->lastHeaders, $h);
		else
			$this->lastHeaders = $h;
			
		return $this;
	}
	
	public function addHeader($key, $val){
	
		$this->lastHeaders[$key] = $val;
	
	}
	
	public function removeHeader($key){
	
		if (isset($this->lastHeaders[$key]))
			unset($this->lastHeaders[$key]);
	}
	
	/**
	 * Response code
	 *
	 * Get the response code for the last request
	 *
	 * @return $this
	 */
	public function responseCode(){
		
		$request = $this->lastRequest();
		
		if (isset($request['http_code']))
			return $request['http_code'];
			
		return false;
	
	}
	
	/**
	 * Get info about the last request
	 *
	 * @return array $info
	 */
	public function lastRequest(){
	
		return $this->lastInfo;
	}
	
	/**
	 * DELETE request
	 *
	 * Sends a DELETE request to either the base url (configured in settings.json), or
	 * the extended url, which is configured using the self::location($url) method in 
	 * this class.
	 *
	 * @return mixed $response - The response receieved from the web server
	 */
	public function delete(){

		$c = $this->getHandle('delete');
		
		curl_setopt($c['handle'], CURLOPT_CUSTOMREQUEST, "DELETE");
		
		$response = curl_exec($c['handle']);
		$headerSize = curl_getinfo($c['handle'], CURLINFO_HEADER_SIZE);
		$headers = substr($response, 0, $headerSize);
		$body = substr($response, $headerSize);
		
		if ($this->accepts('json') && $this->autoConvert)
			return json_decode($body, true);
			
		return $body;
	
	}
	
	public function cookieHistory(){
	
		return $this->cookieHistory;
	}
	
	public function autoSetCookies($bool=true){
	
		$this->autoSetCookies = (boolean) $bool;
	
	}
	
	public function setCookiesFromHeaders($handle, $headers){
		
		if ($this->autoSetCookies){
			
			$cookies = array();
			preg_match_all('|Set-Cookie: (.*);|U', $headers, $cookies);    
			
			foreach ($cookies[1] as $str){
				
				if (strpos($str, '=') === false)
					continue;
				
				list($cookieName, $cookieVal) = explode('=', $str, 2);
				$this->cookieHistory[] = array($cookieName => $cookieVal);
			}
			
			$cookies = implode(';', $cookies[1]);
			
			curl_setopt($handle, CURLOPT_COOKIE,  $cookies);
		
		}
		
		return $this;
	
	}
	
	/**
	 * Do Async
	 *
	 * To DO: Current not implemented.
	 *
	 * @param optional string $method - The HTTP request method
	 * @param optional array $params - An array of key value pairs to pass in the request
	 *
	 * @return mixed $response - The response receieved from the web server
	 */
	private function doAsync($method='get', array $params=array()){
		
		throw new Exception('Async is currently not supported');
		
	}
	
	/**
	 * Don't wait
	 *
	 * Performs an HTTP request without waiting for a response
	 *
	 * @param optional string $method - The HTTP request method
	 * @param optional array $params - An array of key value pairs to pass in the request
	 *
	 * @return integer $bytes - The number of bytes written
	 */
	private function dontWait($method='get', array $params=array()){
		
		$c = $this->getHandle($method);
		$url = $c['address'];
		$paramStr = "";
				
		if ($params) {
		
			$tmp = array();
			
			foreach ($params as $key => &$val) {
			  if (is_array($val)) $val = implode(',', $val);
				$tmp[] = $key.'='.urlencode($val);
			}
			
			$paramStr = implode('&', $tmp);
		
		}
		
		$parts = parse_url($url);
		
		if ($method == 'get' && $params){
			$parts['path'] .= '?' . http_build_query($params);
		}
		
		$fp = @fsockopen($parts['host'],
						 isset($parts['port']) ? $parts['port'] : 80,
						 $errno, 
						 $errstr, 
						 30);
			
		if (!$fp)
			return false;
			
		$out = strtoupper($method) . " ".$parts['path']." HTTP/1.1\r\n";
		$out.= "Host: ".$parts['host'].":".$parts['port']."\r\n";
		$out.= "Content-Type: application/x-www-form-urlencoded\r\n";
		$out.= "Content-Length: ".strlen($paramStr)."\r\n";
		$out.= "Connection: Close\r\n\r\n";
		if (isset($paramStr)) $out.= $paramStr;

		$bytes = fwrite($fp, $out);
		fclose($fp);
		
		return $bytes;
		
	}
	
	/**
	 * GET request
	 *
	 * Sends a GET request to either the base url (configured in settings.json), or
	 * the extended url, which is configured using the self::location($url) method in 
	 * this class.
	 * 
	 * @param optional array $data - An optional array of data to be sent with the request
	 * @return mixed $response - The response receieved from the web server
	 */
	public function get(array $data=array()){
		
		if ($this->async){
			return $this->doAsync('get', $data);
		} else if (!$this->wait){
			return $this->dontWait('get', $data);
		}
		
		$c = $this->getHandle('get');
		
		$data ? $url = $c['address'] . '?' . http_build_query($data) : $url = $c['address'];
		
		curl_setopt($c['handle'], CURLOPT_CUSTOMREQUEST, "GET");
		curl_setopt($c['handle'], CURLOPT_URL, $url);
	
		$response = curl_exec($c['handle']);
		$headerSize = curl_getinfo($c['handle'], CURLINFO_HEADER_SIZE);
		$headers = substr($response, 0, $headerSize);
		$body = substr($response, $headerSize);
		
		$this->setCookiesFromHeaders($c['handle'], $headers);
		
		$this->lastInfo = curl_getinfo($c['handle']);
		$this->lastInfo['headers_sent'] = $this->headersSent;
		$this->lastInfo['headers_resp_str'] = $headers;
		$this->lastInfo['headers_resp'] = $this->extractHeaders($headers);
		
		if ($this->accepts('json') && $this->autoConvert){
			
			$body = json_decode($body, true);
			
			// HBase encodes the data in base64, so we need to loop through and decode these values
			if (isset($body['Row'])){
			
				foreach ($body['Row'] as $index => $row){
				
					// decode the row key
					$row['key'] = base64_decode($row['key']);
					
					array_walk($row['Cell'], function(&$r){
						
						// decode the column
						if (isset($r['column']))
							$r['column'] = base64_decode($r['column']);
						
						// HBase puts the cell data in the $ index when using JSON
						if (isset($r['$']))
							$r['$'] = base64_decode($r['$']);
							
					});
					
					$body['Row'][$index] = $row;
				}
			}
			
		} else 	if ($this->accepts('xml') && $this->autoConvert){
			
			$dom = new DOMDocument($this->xmlVersion);
			$dom->loadXML($body);
			$dom->saveXML();
			return $dom;
		}
		
		return $body;
	
	}
	
	/**
	 * POST request
	 *
	 * Sends a post request to either the base url (configured in settings.json), or
	 * the extended url, which is configured using the self::location($url) method in 
	 * this class.
	 *
	 * @param optional array $data - An optional array of data to be sent with the request
	 * @return mixed $response - The response receieved from the web server
	 */
	public function post($data=array(), $skipEncoding=false){
		
		if ($this->async){
			return $this->doAsync('post', $data);
		} else if (!$this->wait){
			return $this->dontWait('post', $data);
		}
		
		$c = $this->getHandle('post');
		
		if (!$skipEncoding)
			$dataStr = http_build_query($data);
		else
			$dataStr = $data;
		
		curl_setopt($c['handle'], CURLOPT_CUSTOMREQUEST, "POST");
		curl_setopt($c['handle'], CURLOPT_POSTFIELDS, $dataStr);
		
		$response = curl_exec($c['handle']);
		$headerSize = curl_getinfo($c['handle'], CURLINFO_HEADER_SIZE);
		$headers = substr($response, 0, $headerSize);
		$body = substr($response, $headerSize);
		
		$this->lastInfo = curl_getinfo($c['handle']);
		$this->lastInfo['custom_headers'] = $this->headersSent;
		$this->lastInfo['headers_resp_str'] = $headers;
		$this->lastInfo['headers_resp'] = $this->extractHeaders($headers);
		$this->lastInfo['post_data'] = $data;
		$this->lastInfo['post_data_str'] = $dataStr;
		
		if ($this->accepts('json') && $this->autoConvert)
			return json_decode($body, true);
			
		return $body;
	
	}
	
	/**
	 * PUT request
	 *
	 * Sends a PUT request to either the base url (configured in settings.json), or
	 * the extended url, which is configured using the self::location($url) method in 
	 * this class.
	 *
	 * @return mixed $response - The response receieved from the web server
	 */
	public function put($data=array(), $skipEncoding=false){
		
		if ($this->async){
			return $this->doAsync('put', $data);
		} else if (!$this->wait){
			return $this->dontWait('put', $data);
		}
		
		$c = $this->getHandle('put');
		
		if (!$skipEncoding)
			$dataStr = http_build_query($data);
		else
			$dataStr = $data;
		
		curl_setopt($c['handle'], CURLOPT_PUT, 1);
		curl_setopt($c['handle'], CURLOPT_POSTFIELDS, $dataStr);
		
		$response = curl_exec($c['handle']);
		$headerSize = curl_getinfo($c['handle'], CURLINFO_HEADER_SIZE);
		$headers = substr($response, 0, $headerSize);
		$body = substr($response, $headerSize);
		
		$this->lastInfo = curl_getinfo($c['handle']);
		$this->lastInfo['custom_headers'] = $this->headersSent;
		$this->lastInfo['headers_resp_str'] = $headers;
		$this->lastInfo['headers_resp'] = $this->extractHeaders($headers);
		$this->lastInfo['put_data'] = $data;
		$this->lastInfo['put_data_str'] = $dataStr;
		
		if ($this->accepts('json') && $this->autoConvert)
			return json_decode($body, true);
			
		return $body;
	
	}
	
	/**
	 * Binary
	 *
	 * Sets the appropriate header for retrieving binary responses
	 * from the server
	 *
	 * @return $this
	 */
	public function binary(){
		
		$this->acceptHeader = 'application/octet-stream';
		return $this;
	}
	
	/**
	 * HTML
	 *
	 * Sets the appropriate header for retrieving html text responses
	 * from the server
	 *
	 * @return $this
	 */
	public function html(){
		
		$this->acceptHeader = 'text/html';
		$this->contentType  = '';
		return $this;
	}
	
	/**
	 * JSON
	 *
	 * Sets the appropriate header for retrieving json responses
	 * from the server
	 *
	 * @return $this
	 */
	public function json($accepts=true, $contentType=false){
		
		if ($contentType)
			$this->contentType  = 'application/json';
			
		if ($accepts)
			$this->acceptHeader = 'application/json';

		return $this;
	}
	
	/**
	 * Plain text
	 *
	 * Sets the appropriate header for retrieving plain text responses
	 * from the server
	 *
	 * @return $this
	 */
	public function plain(){
		
		$this->acceptHeader = 'text/plain';
		$this->contentType  = '';
		return $this;
	}
	
	/**
	 * Protocol Buffer
	 *
	 * Sets the appropriate header for retrieving protobuf (Protocol Buffer) 
	 * responses from the server
	 *
	 * @return $this
	 */
	public function protobuf(){
		
		$this->acceptHeader  = 'application/x-protobuf';
		$this->contentType  = '';
		return $this;
	}

	/**
	 * XML
	 *
	 * Sets the appropriate header for retrieving xml responses
	 * from the server
	 *
	 * @return $this
	 */
	public function xml($version="1.0"){
		
		$this->acceptHeader = 'text/xml';
		$this->contentType  = 'text/xml';
		$this->xmlVersion   = $version;
		return $this;
	}

}

?>