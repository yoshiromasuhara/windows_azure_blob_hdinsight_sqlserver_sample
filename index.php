<?php
	set_time_limit(0);

	require_once 'config.php';

	//SDKの読み込み
	require_once 'WindowsAzure\WindowsAzure.php';
	use WindowsAzure\Common\ServicesBuilder;
	use WindowsAzure\Common\ServiceException;

	//BLOBストレージ接続文字列
	$connectionString = 'DefaultEndpointsProtocol='.BLOB_DEFAULTENDPOINTPROTOCOL.';'
						.'AccountName='.BLOB_ACCOUNTNAME.';'
						.'AccountKey='.BLOB_ACCOUNTKEY.'';

	$blobRestProxy = ServicesBuilder::getInstance()->createBlobService($connectionString);

	//-■■■■■-ローカルのデータを取り込んでBLOBファイルにアップロード-■■■■■-
    $blobRestProxy->createBlockBlob(BLOB_CONTAINER, 'sample/nikkei_avg255.csv', file_get_contents("data/nikkei_avg255.csv"));


    //-■■■■■-HDInsightにデータ登録-■■■■■-
	$output_path="sample/output/";

	$ch = curl_init(HD_ENDPOINT);

	// 返り値を文字列として受け取る
	curl_setopt($ch, CURLOPT_RETURNTRANSFER, TRUE);
	curl_setopt($ch, CURLOPT_USERPWD, HD_USERID.":".HD_USERPW);
	//CA証明書の検証をしない
	curl_setopt($ch,CURLOPT_SSL_VERIFYPEER,false);
	curl_setopt($ch, CURLOPT_VERBOSE, true);
	// POSTするデータをセット
	curl_setopt($ch,CURLOPT_HTTPHEADER, array('Content-type: application/x-www-form-urlencoded'));
	curl_setopt($ch,CURLOPT_POST ,1);

	//テーブル削除
	$HiveQL="DROP TABLE NIKKEI255_RAW";
	curl_setopt($ch,CURLOPT_POSTFIELDS, "user.name=".HD_USERID."&execute=".$HiveQL.";&statusdir=".$output_path);
	$result = curl_exec($ch);
	$rs=json_decode($result);
	$job_id=$rs->id;
	checkJob($job_id);

	//テーブル作成およびデータ投入
	$HiveQL ="CREATE EXTERNAL TABLE NIKKEI255_RAW (";
	$HiveQL.="C_DATE string,STA_PRICE float,MAX_PRICE float,MIN_PRICE float,END_PRICE float) ";
	$HiveQL.="row format delimited  fields terminated by ','  lines terminated by '\\n'  stored as textfile  location ";
	$HiveQL.="'asv://".BLOB_CONTAINER."@".BLOB_ACCOUNTNAME.".blob.core.windows.net/sample/'";
	curl_setopt($ch,CURLOPT_POSTFIELDS, "user.name=".HD_USERID."&execute=".$HiveQL.";&statusdir=".$output_path);
	$result = curl_exec($ch);
	$rs=json_decode($result);
	$job_id=$rs->id;
	checkJob($job_id);

	//不要データ以外をファイルに書き込み
	$HiveQL ="INSERT OVERWRITE LOCAL DIRECTORY '/user/skawasaki/sample/result/' SELECT * FROM NIKKEI255_RAW ";
	$HiveQL.="WHERE END_PRICE is not null ";
	curl_setopt($ch,CURLOPT_POSTFIELDS, "user.name=".HD_USERID."&execute=".$HiveQL.";&statusdir=".$output_path);
	$result = curl_exec($ch);
	$rs=json_decode($result);
	$job_id=$rs->id;
	checkJob($job_id);

	curl_close($ch);

    //-■■■■■-HDInsightが登録したBLOBデータを読み込み-■■■■■-
   	$blob_list = $blobRestProxy->listBlobs(BLOB_CONTAINER);
    $blobs = $blob_list->getBlobs();

    $calc_data_raw="";
    $calc_data=array();
    foreach($blobs as $blob){
    	if(strstr($blob->getName(),'user/skawasaki/sample/result/') ){
	        $calc_data_raw=str_replace("\001",",",file_get_contents($blob->getUrl()));
	        $calc_data=explode("\n", $calc_data_raw);
    	}
    }

	//-■■■■■-SQLServer接続-■■■■■-
	$conn = new PDO ("sqlsrv:server = tcp:".SQLSRV_HOST.",1433; Database = ".SQLSRV_DATABASE, SQLSRV_USER, SQLSRV_PW);
	$conn->setAttribute( PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION );
	//既存レコード初期化
	$stmt = $conn->prepare("DELETE FROM NIKKEI255");
	$stmt->execute();
	//データ追加
	$sql="INSERT INTO NIKKEI255(C_DATE,STA_PRICE,MAX_PRICE,MIN_PRICE,END_PRICE)VALUES";
	$sql_add="";
	$row_cnt=0;
	foreach ($calc_data as $recs) {
		$rec=explode(",",$recs);
		if(count($rec)!=5){
			continue;
		}
		if($sql_add!=""){
			$sql_add.=",";
		}
		$sql_add.="('".$rec[0]."',".$rec[1].",".$rec[2].",".$rec[3].",".$rec[4].")";
		$row_cnt++;

		if($row_cnt==1000){
			$stmt = $conn->prepare($sql.$sql_add);
			$stmt->execute();
			$sql_add="";
			$row_cnt=0;
		}
	}

	$stmt = $conn->prepare($sql.$sql_add);
	$stmt->execute();

	exit;

	//-■■■■■-ジョブ完了チェック（再帰）-■■■■■-
	function checkJob($job_id){

		$ch = curl_init(HD_ENDPOINT_JOB.$job_id."?user.name=".HD_USERID);

		// 返り値を文字列として受け取る
		curl_setopt($ch, CURLOPT_RETURNTRANSFER, TRUE);
		curl_setopt($ch, CURLOPT_USERPWD, HD_USERID.":".HD_USERPW);
		//CA証明書の検証をしない
		curl_setopt($ch,CURLOPT_SSL_VERIFYPEER,false);
		curl_setopt($ch, CURLOPT_VERBOSE, true);
		$result = curl_exec($ch);
		$rs=json_decode($result);

		if($rs->completed!="done"){
			sleep(1);
			checkJob($job_id);
		}
	}
?>