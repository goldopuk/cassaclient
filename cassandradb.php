<?php

require_once('phpcassa/connection.php');
require_once('phpcassa/columnfamily.php');

class CassandraDB {
	
	private $conn;
    private $cf, $CI;   

    public $result = array();

    private $count = 0;

	 public function __construct() {
        $this->CI = & get_instance();
        $this->cf = $this->CI->config->item('default_cf');        
        $this->createPool($this->CI->config->item('keyspace'), $this->CI->config->item('cassandra_servers'));
    }

    /**
     * Provides a standard connection method to the database directly.
     * The function connect to an existent cassandra KEYSPACE(database)
     *
     * @param $keyspace
     * @param $servers
     */
    private function createPool($keyspace, $servers=NULL) 
    {
        try {
            $this->conn = new ConnectionPool($keyspace, $servers);
        } catch (Exception $e) {
            show_error($e->getMessage());
        }
    }

    /**
     * Provides a simple way to run queries on a cassandra KEYSPACE.
     *
     * @param $cql
     */
    public function query($cql) 
    {
    	if ($cql != "") {
    		$raw = $this->conn->get();
    		$rows = $raw->client->execute_cql_query($cql, 2);
    		$this->conn->return_connection($raw);

            return $this->formatQueryData($rows);
            // return $rows;
    	} else {
    		die('OHFUCK, I CANNOT DO A QUERY, IM STUPID!');
    	}

    } 


    public function formatQueryData($datasource)
    {        
        if ($datasource != "") {
            foreach ($datasource->rows as $row => $val) {                
                $this->result[$this->count] = $val->columns;
                ++$this->count;
            }
        }

        return $this->result;
    }



}