public class CoprocessorLin extends BaseRegionObserver{
@Override
	  public void postPut(final ObserverContext<RegionCoprocessorEnvironment> e, 
	      final Put put, final WALEdit edit, final Durability durability) throws IOException {
		//judge if has the kind of put which is our put operation
			if(put.has("event".getBytes(), "la".getBytes())){
				
				Connection connect = ConnectionFactory.createConnection(e.getEnvironment().getConfiguration());			
				//get the table which is processing now
				Table processTable = connect.getTable(e.getEnvironment().getRegionInfo().getTable());
				//get the value which is the front rowkey corresponded to.
				String preValue = getPreValue(processTable, put);
				
				//get the key and value which is putting now!
				List<Cell> kv = put.get("event".getBytes(), "la".getBytes());//Get the whole cells when client set out the operation of getting
				Iterator<Cell> kvItor = kv.iterator();			
				String  nowPutvalue=new String();
				String nowPutkey = new String();
				while (kvItor.hasNext()) {
					 Cell tmp = kvItor.next();
					 nowPutkey = CellUtil.getCellKeyAsString(tmp);
					 nowPutvalue = CellUtil.cloneValue(tmp).toString();
				}
				
				//calculate the status by comparing the value of pre and now.
				String status = Status.calStatus(preValue, nowPutvalue);//calculate the status of this car;
				
				Put indexPut = new Put((nowPutkey.substring(14, 23)+status).getBytes());
				indexPut.addColumn("status".getBytes(), nowPutkey.substring(24,nowPutkey.length()-1).getBytes(), nowPutkey.getBytes());
				
				//table which is using for indexing object.
				Table table = connect.getTable(TableName.valueOf("indexTable".getBytes()));
				table.put(indexPut);
				table.close();
			}
		}
}