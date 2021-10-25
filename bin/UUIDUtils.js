// Copied from: https://github.com/xehrad/UUID_to_Date/blob/master/UUID_to_Date.js

class UUIDToDate {
  constructor() {
  	this.GREGORIAN_OFFSET = 122192928000000000;
  }

  extract(uuid_str) {
    return this.get_date_obj(uuid_str);
	}

  get_date_obj(uuid_str) {
    // (string) uuid_str format	=>		'11111111-2222-#333-4444-555555555555'
    var int_time = this.get_time_int( uuid_str ) - this.GREGORIAN_OFFSET,
      int_millisec = Math.floor( int_time / 10000 );
    return new Date( int_millisec );
  }

  get_time_int(uuid_str) {
    // (string) uuid_str format	=>		'11111111-2222-#333-4444-555555555555'
    var uuid_arr = uuid_str.split( '-' ),
      time_str = [
        uuid_arr[ 2 ].substring( 1 ),
        uuid_arr[ 1 ],
        uuid_arr[ 0 ]
      ].join( '' );
      // time_str is convert  '11111111-2222-#333-4444-555555555555'  to  '333222211111111'
    return parseInt( time_str, 16 );
  }

}

const uuidToDate = new UUIDToDate();
module.exports = {uuidToDate};
