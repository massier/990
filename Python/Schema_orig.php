<?php

class Model_DbTable_Irs_Schema extends Model_DbBaseClass
{
	protected $_name = 'schema';
	protected $_primary = 'id';
	protected $_useAdapter = 'irs';

	public $id = null;
    public $year = null;
    public $form = null;
    public $version = null;
    public $highestLevel = null;
    public $tableName = null;
    public $path = null;
    public $dataType = null;
    public $maxLength = null;
    public $minOccurs = null;
    public $description = null;
    public $pattern = null;
    public $itemList = null;

    public $searchData = array();

    public function getDataType($table, $field = null)
    {
        if(!$field)
        {
            $select = $this->select()
                ->from('irs.schema', array('dataType', 'fieldName'))
                ->where("tableName = ?", $table);
            $rowset = $this->fetchAll($select);
            return $rowset->toArray();
        }
        $select = $this->select()
            ->from('irs.schema', array('dataType'))
            ->where("tableName = ?", $table)
            ->where("fieldName = ?", $field)
            ->limit(1);
        $row = $this->fetchRow($select);
        if($row)
        {
            return $row->dataType;
        }
        return null;
    }

    public function loadForSearch()
    {
        $this->searchData = array(
            'years' => array(),
            'forms' => array()
        );
        $select = $this->select()
                    ->setIntegrityCheck(false)
                    ->from(array('i' => 'irs.schema'), array('year' => "DISTINCT(year)"))
                    ->where("ISNULL(dataType)")
                    ->where("highestLevel = ?", 'ReturnData')
                    ->order('year');
        $rowset = $this->fetchALl($select);
        foreach($rowset as $row)
        {
            $this->searchData['years'][] = $row->year;
        }

        $select = $this->select()
                    ->setIntegrityCheck(false)
                    ->from(array('i' => 'irs.schema'), array('form' => "DISTINCT(form)", 'year'))
                    ->where("ISNULL(dataType)")
                    ->where("highestLevel = ?", 'ReturnData')
                    ->order(array('year', 'form'));

        $rowset = $this->fetchALl($select);
        foreach($rowset as $row)
        {
            if(!isset($this->searchData['forms'][$row->form]))
            {
                $this->searchData['forms'][$row->form] = array();
            }
            $this->searchData['forms'][$row->form][] = $row->year;
        }

    }

    public function getSections(Model_DbTable_Irs_DashboardCriteria $criteria)
    {
        $yearString = implode("','", $criteria->years);
        $formString = implode("','", $criteria->forms);
        $select = $this->select()
                    ->setIntegrityCheck(false)
                    ->from(array('i' => 'irs.schema'), array('tableName' => "DISTINCT(tableName)", 'year', 'form'))
                    ->where("ISNULL(dataType)")
                    ->where("highestLevel = ?", 'ReturnData')
                    ->order(array('year', 'form', 'tableName'));
        if(strlen($yearString))
        {
            $select->where("year IN('$yearString')");
        }
        if(strlen($formString))
        {
            $select->where("form IN('$formString')");
        }
        $rowset = $this->fetchALl($select);
        $returnArray = array();
        foreach($rowset as $row)
        {
            $tableName = $this->parseCamelCase($row->tableName);
            if(!in_array($tableName, $returnArray))
            {
                $returnArray[$row->tableName] = $tableName;
            }
        }
        return $returnArray;
    }

    public function getFields(Array $tables)
    {
        $returnArray = array();
        $tableString= implode("','", $tables);
        $select = $this->select()
                    ->setIntegrityCheck(false)
                    ->from(array('i' => 'irs.schema'), array('fieldName' => "DISTINCT(fieldName)", 'tableName'))
                    ->where("highestLevel = ?", 'ReturnData')
                    ->where("!ISNULL(dataType)")
                    ->where("year IN('2013', '2014')")
                    ->where("form = '990'")
                    ->where("tableName IN('$tableString')")
                    ->order(array('tableName', 'fieldName'));

        $rowset = $this->fetchALl($select);
        foreach($rowset as $row)
        {
            $returnArray[] = array('key' => $row->tableName . '_' . $row->fieldName, 'value' => $this->parseCamelCase($row->tableName) . ' - ' . $this->parseCamelCase($row->fieldName));
        }
        return $returnArray;
    }

    private function parseCamelCase($string)
    {
        return ucwords(preg_replace(array('/(?<=[^A-Z])([A-Z])/', '/(?<=[^0-9])([0-9])/'), ' $0', $string));
    }

    public function processXml(Model_DbTable_Irs_Filing $filing)
    {
        set_time_limit(600);
        if(!$this->haveSchema($filing->year, $filing->formType, $filing->version))
        {
            if($filing->formType != '990' && $filing->formType != '990EZ')
            {
                Model_DbTable_Irs_Log::log(Model_DbTable_Irs_Log::$LOGLEVELS['warn'], 'invalid form type ' . $filing->id . ' ' . $filing->formType);
                return false;
            }
            if($filing->year < 2012 || $filing->year > 2014)
            {
                Model_DbTable_Irs_Log::log(Model_DbTable_Irs_Log::$LOGLEVELS['warn'], 'unsupported year ' . $filing->id . ' ' . $filing->year);
                return false;
            }
            if($filing->year == '2012')
            {
                $version = '3.0';
            }
            elseif($filing->year == '2013')
            {
                $version = '4.0';
            }
            elseif($filing->year == '2014')
            {
                $version = '6.0';
            }
        }
        $select = $this->select()
                    ->from('irs.schema', array('tableName' => "IF(nccsTable, nccsTable, tableName)", 'path'))
                    ->where("year = ?", $filing->year)
                    ->where("form = ?", $filing->formType)
                    ->where("version = ?", $version)
                    ->group('tableName');
        $rowset = $this->fetchAll($select);
        $data = array();
        if(!count($rowset))
        {
            Model_DbTable_Irs_Log::log(Model_DbTable_Irs_Log::$LOGLEVELS['warn'], 'unable to locate schema for - year: ' . $filing->year . ' form:' . $filing->formType . ' version:' . $version);
            return false;
        }

        $multiples = array();
        $singles = array();
        foreach($rowset as $row)
        {
            $path = '//default:' . str_replace('/', '/default:', $row->path);
            $value = $filing->xml->xpath($path);
            if(count($value) > 1)
            {
                if(!in_array($row->tableName, $multiples))
                {
                    $multiples[] = $row->tableName;
                }
            }
            else
            {
                if(!in_array($row->tableName, $singles))
                {
                    $singles[] = $row->tableName;
                }
            }
        }
        foreach($singles as $table)
        {
            $select = $this->select()
                        ->where("year = ?", $filing->year)
                        ->where("form = ?", $filing->formType)
                        ->where("version = ?", $version)
                        ->where("nccsTable = '$table' OR tableName = '$table'");
            $rowset = $this->fetchAll($select);
            foreach($rowset as $row)
            {
                $rowTable = ($row->nccsTable) ? $row->nccsTable : $row->tableName;

                if(!isset($data[$rowTable]))
                {
                    $data[$rowTable] = array(0 => array());
                }
                if(strlen($row->nccsTable))
                {
                    $fieldName = $row->tableName . '_' . $row->fieldName;
                }
                else
                {
                    $fieldName = $row->fieldName;
                }
                $path = '//default:' . str_replace('/', '/default:', $row->path);
                $value = $filing->xml->xpath($path);

                if(isset($value[0]))
                {
                    $value = trim((string)$value[0]);
                    if(strlen($value))
                    {
                        $data[$rowTable][0][$fieldName] = $value;
                    }
                }
            }
        }
        foreach($multiples as $table)
        {
            $select = $this->select()
                        ->where("year = ?", $filing->year)
                        ->where("form = ?", $filing->formType)
                        ->where("version = ?", $version)
                        ->where("nccsTable = '$table' OR tableName = '$table'");
            $rowset = $this->fetchAll($select);
            foreach($rowset as $row)
            {
                $path = '//default:' . str_replace('/', '/default:', $row->path);
                $values = $filing->xml->xpath($path);
                $rowTable = ($row->nccsTable) ? $row->nccsTable : $row->tableName;
                if(!isset($data[$rowTable]))
                {
                    $data[$rowTable] = array();
                }
                if(strlen($row->nccsTable))
                {
                    $fieldName = $row->tableName . '_' . $row->fieldName;
                }
                else
                {
                    $fieldName = $row->fieldName;
                }
                $counter = 0;
                foreach($values as $value)
                {
                    if(!isset($data[$rowTable][$counter]))
                    {
                        $data[$rowTable][$counter] = array();
                    }
                    $v = trim((string)$value[0]);
                    if(strlen($v))
                    {
                        $data[$rowTable][$counter][$fieldName] = $v;
                    }
                    $counter++;
                }
            }
        }

        //var_dump($data['AdvertisingGrp']);
        //echo '<hr/>';
//var_dump($data['Form990PartVIISectionAGrp']);


        foreach($data as $table => $fullArray)
        {
            foreach($fullArray as $arr)
            {
                if(count($arr))
                {
                    $arr['fk'] = $filing->id;
                    //$id = null;
                    //$select = $this->select()
                        //->setIntegrityCheck(false)
                        //->from($table, array('id'))
                        //->where("fk = ?", $filing->id)
                        //->limit(1);
                    //$row = $this->fetchRow($select);
                    //if($row)
                    //{
                        //$id = $row->id;
                    //}
                    $db = new Zend_Db_Table('irs.' . $table);
                    //if($id)
                    //{
                        //$where = "id = '$id'";
                        //$db->update($arr, $where);
                    //}
                    //else {
                        $arr["id"] = null;
                        $id  = $db->insert( $arr );
                    //}
                }
            }
        }
        Model_DbTable_Irs_Log::log(Model_DbTable_Irs_Log::$LOGLEVELS['notice'], 'processed filing: ' . $filing->id);
        return true;
    }

    public function haveSchema($year, $form, $version)
    {
        $select = $this->select()
            ->where("year = ?", $year)
            ->where("form = ?", $form)
            ->where("version = ?", $version)
            ->limit(1);
        $row = $this->fetchRow($select);
        if($row)
        {
            return true;
        }
        return false;
    }

    public function processSchemaFile($file, $year, $form, $version)
    {
        if(is_file($file))
        {
            $xml = simplexml_load_file($file);
            $header = $xml->Definition->Definition[1];
            $data = $xml->Definition->Definition[2];

            foreach($header as $definition)
            {
                $this->processDefinition($definition, 'ReturnHeader', $year, $form, $version);
            }
            foreach($data as $definition)
            {
                $this->processDefinition($definition, 'ReturnData', $year, $form, $version);
            }
        }

    }

    private function processDefinition($definition, $highestLevel, $year, $form, $version)
   	{
   		$arr = explode('/', $definition['Name']);
   		$itemList = '';
   		if(isset($definition->ItemList))
   		{
   			foreach($definition->ItemList as $item)
   			{
   				$itemList .= ($itemList) ? '|' : '';
   				$itemList .= $item['Name'];
   			}
   		}
   		$schema = new Model_DbTable_Irs_Schema();
        $schema->year = $year;
        $schema->form = $form;
        $schema->version = $version;
   		$schema->highestLevel = $highestLevel;
   		$schema->tableName = $arr[count($arr) - 2];
   		$schema->path = $definition['Name'];
   		$schema->dataType = (isset($definition->DataType)) ? $definition->DataType : null;
   		$schema->maxLength = (isset($definition->MaxLength)) ? $definition->MaxLength : null;
   		$schema->minOccurs = (isset($definition->MinOccurs)) ? $definition->MinOccurs : null;
   		$schema->description = (isset($definition->Description)) ? $definition->Description : null;
   		$schema->pattern = (isset($definition->Pattern)) ? $definition->Pattern : null;
   		$schema->itemList = (strlen($itemList)) ? $itemList : null;
   		$schema->save();
   		if(isset($definition->Definition))
   		{
   			foreach($definition->Definition as $definition)
   			{
   				$this->processDefinition($definition, $highestLevel, $year, $form, $version);
   			}
   		}
   	}

    public function createTables()
    {
        $db = $this->getAdapter();
        $select = $this->select()
            ->where("!ISNULL(dataType)");
        $rowset = $this->fetchAll($select);
        $rows = array();
        foreach($rowset as $row)
        {
            $rowTable = ($row->nccsTable) ? $row->nccsTable : $row->tableName;
            if(!isset($rows[$rowTable]))
            {
                $rows[$rowTable] = array();
            }
            $rows[$rowTable][] = $row;
        }
        ksort($rows);
        foreach($rows as $tableName => $arr)
        {
            $sql = "DROP TABLE IF EXISTS `$tableName`";
            $db->query($sql);
            $sql = "CREATE TABLE `$tableName` (
              `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
              `fk` int(10) unsigned DEFAULT NULL,";
            $fieldsUsed = array();
            foreach($arr as $row)
            {
                $fieldName = $row->fieldName;
                if(strlen($row->nccsTable))
                {
                    $fieldName = $row->tableName . '_' . $row->fieldName;
                }
                if(!in_array($fieldName, $fieldsUsed))
                {
                    $fieldsUsed[] = $fieldName;
                    $sql .= " `$fieldName` ";
                    if($row->dataType == 'Decimal')
                    {
                        $sql .= "decimal(12,4) DEFAULT NULL,";
                    }
                    if($row->dataType == 'DateTime')
                    {
                        $sql .= "datetime DEFAULT NULL,";
                    }
                    if($row->dataType == 'Boolean')
                    {
                        $sql .= "tinyint(4) DEFAULT NULL,";
                    }
                    if($row->dataType == 'String')
                    {
                        $max = ($row->maxLength) ? $row->maxLength : 255;
                        $sql .= "varchar($max) DEFAULT NULL,";
                    }
                    if($row->dataType == 'String[]')
                    {
                        $sql .= "varchar(255) DEFAULT NULL,";
                    }
                }
            }
            $sql .= " PRIMARY KEY (`id`) )";
            $db->query($sql);
        }
    }

    public function createIndexes()
        {
            $db = $this->getAdapter();
            $select = $this->select()
                ->where("!ISNULL(dataType)");
            $rowset = $this->fetchAll($select);
            $rows = array();
            foreach($rowset as $row)
            {
                $rowTable = ($row->nccsTable) ? $row->nccsTable : $row->tableName;
                if(!isset($rows[$rowTable]))
                {
                    $rows[$rowTable] = array();
                }
                $rows[$rowTable][] = $row;
            }
            ksort($rows);
            foreach($rows as $tableName => $arr)
            {
                $sql = "ALTER TABLE `$tableName` ADD INDEX `fk` (`fk`)";
                $db->query($sql);
            }
        }

    public function __construct($id = null)
    {
        parent::__construct();
        if ($id)
        {
            $this->id = $id;
            $this->load();
        }
    }

    public function load($row = null)
    {
        if(!$row)
        {
            $select = $this->select()
                ->where("id = ?", $this->id);
            $row = $this->fetchRow($select);
        }
        if ($row)
        {
            $this->id = $row->id;
            $this->year = $row->year;
            $this->form = $row->form;
            $this->version = $row->version;
            $this->highestLevel = $row->highestLevel;
            $this->tableName = $row->tableName;
            $this->path = $row->path;
            $this->dataType = $row->dataType;
            $this->maxLength = $row->maxLength;
            $this->minOccurs = $row->minOccurs;
            $this->description = $row->description;
            $this->pattern = $row->pattern;
            $this->itemList = $row->itemList;
        }
    }

    public function save()
    {
        $data = array(
            'year' => $this->year,
            'form' => $this->form,
            'version' => $this->version,
            'highestLevel' => $this->highestLevel,
            'tableName' => $this->tableName,
            'path' => $this->path,
            'dataType' => $this->dataType,
            'maxLength' => $this->maxLength,
            'minOccurs' => $this->minOccurs,
            'description' => $this->description,
            'pattern' => $this->pattern,
            'itemList' => $this->itemList
        );
        if($this->id)
        {
            $where = "id = '$this->id'";
            $this->update($data, $where);
        }
        else
        {
            $data["id"] = null;
            $this->id = $this->insert($data);
        }
    }

}
