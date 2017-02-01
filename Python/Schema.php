<?php

class Model_DbTable_Irs_Schema #extends Model_DbBaseClass
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
        #if($this->id)
        #{
        #    $where = "id = '$this->id'";
        #    $this->update($data, $where);
        #}
        #else
        #{
        #    $data["id"] = null;
        #    $this->id = $this->insert($data);
        #}

    }

}


