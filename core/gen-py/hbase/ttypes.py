#
# Autogenerated by Thrift Compiler (0.9.0)
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#
#  options string: py
#

from thrift.Thrift import TType, TMessageType, TException, TApplicationException

from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol, TProtocol
try:
  from thrift.protocol import fastbinary
except:
  fastbinary = None



class TCell:
  """
  TCell - Used to transport a cell value (byte[]) and the timestamp it was
  stored with together as a result for get and getRow methods. This promotes
  the timestamp of a cell to a first-class value, making it easy to take
  note of temporal data. Cell is used all the way from HStore up to HTable.

  Attributes:
   - value
   - timestamp
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'value', None, None, ), # 1
    (2, TType.I64, 'timestamp', None, None, ), # 2
  )

  def __init__(self, value=None, timestamp=None,):
    self.value = value
    self.timestamp = timestamp

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.value = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.I64:
          self.timestamp = iprot.readI64();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('TCell')
    if self.value is not None:
      oprot.writeFieldBegin('value', TType.STRING, 1)
      oprot.writeString(self.value)
      oprot.writeFieldEnd()
    if self.timestamp is not None:
      oprot.writeFieldBegin('timestamp', TType.I64, 2)
      oprot.writeI64(self.timestamp)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class ColumnDescriptor:
  """
  An HColumnDescriptor contains information about a column family
  such as the number of versions, compression settings, etc. It is
  used as input when creating a table or adding a column.

  Attributes:
   - name
   - maxVersions
   - compression
   - inMemory
   - bloomFilterType
   - bloomFilterVectorSize
   - bloomFilterNbHashes
   - blockCacheEnabled
   - timeToLive
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'name', None, None, ), # 1
    (2, TType.I32, 'maxVersions', None, 3, ), # 2
    (3, TType.STRING, 'compression', None, "NONE", ), # 3
    (4, TType.BOOL, 'inMemory', None, False, ), # 4
    (5, TType.STRING, 'bloomFilterType', None, "NONE", ), # 5
    (6, TType.I32, 'bloomFilterVectorSize', None, 0, ), # 6
    (7, TType.I32, 'bloomFilterNbHashes', None, 0, ), # 7
    (8, TType.BOOL, 'blockCacheEnabled', None, False, ), # 8
    (9, TType.I32, 'timeToLive', None, -1, ), # 9
  )

  def __init__(self, name=None, maxVersions=thrift_spec[2][4], compression=thrift_spec[3][4], inMemory=thrift_spec[4][4], bloomFilterType=thrift_spec[5][4], bloomFilterVectorSize=thrift_spec[6][4], bloomFilterNbHashes=thrift_spec[7][4], blockCacheEnabled=thrift_spec[8][4], timeToLive=thrift_spec[9][4],):
    self.name = name
    self.maxVersions = maxVersions
    self.compression = compression
    self.inMemory = inMemory
    self.bloomFilterType = bloomFilterType
    self.bloomFilterVectorSize = bloomFilterVectorSize
    self.bloomFilterNbHashes = bloomFilterNbHashes
    self.blockCacheEnabled = blockCacheEnabled
    self.timeToLive = timeToLive

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.name = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.I32:
          self.maxVersions = iprot.readI32();
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.STRING:
          self.compression = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 4:
        if ftype == TType.BOOL:
          self.inMemory = iprot.readBool();
        else:
          iprot.skip(ftype)
      elif fid == 5:
        if ftype == TType.STRING:
          self.bloomFilterType = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 6:
        if ftype == TType.I32:
          self.bloomFilterVectorSize = iprot.readI32();
        else:
          iprot.skip(ftype)
      elif fid == 7:
        if ftype == TType.I32:
          self.bloomFilterNbHashes = iprot.readI32();
        else:
          iprot.skip(ftype)
      elif fid == 8:
        if ftype == TType.BOOL:
          self.blockCacheEnabled = iprot.readBool();
        else:
          iprot.skip(ftype)
      elif fid == 9:
        if ftype == TType.I32:
          self.timeToLive = iprot.readI32();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('ColumnDescriptor')
    if self.name is not None:
      oprot.writeFieldBegin('name', TType.STRING, 1)
      oprot.writeString(self.name)
      oprot.writeFieldEnd()
    if self.maxVersions is not None:
      oprot.writeFieldBegin('maxVersions', TType.I32, 2)
      oprot.writeI32(self.maxVersions)
      oprot.writeFieldEnd()
    if self.compression is not None:
      oprot.writeFieldBegin('compression', TType.STRING, 3)
      oprot.writeString(self.compression)
      oprot.writeFieldEnd()
    if self.inMemory is not None:
      oprot.writeFieldBegin('inMemory', TType.BOOL, 4)
      oprot.writeBool(self.inMemory)
      oprot.writeFieldEnd()
    if self.bloomFilterType is not None:
      oprot.writeFieldBegin('bloomFilterType', TType.STRING, 5)
      oprot.writeString(self.bloomFilterType)
      oprot.writeFieldEnd()
    if self.bloomFilterVectorSize is not None:
      oprot.writeFieldBegin('bloomFilterVectorSize', TType.I32, 6)
      oprot.writeI32(self.bloomFilterVectorSize)
      oprot.writeFieldEnd()
    if self.bloomFilterNbHashes is not None:
      oprot.writeFieldBegin('bloomFilterNbHashes', TType.I32, 7)
      oprot.writeI32(self.bloomFilterNbHashes)
      oprot.writeFieldEnd()
    if self.blockCacheEnabled is not None:
      oprot.writeFieldBegin('blockCacheEnabled', TType.BOOL, 8)
      oprot.writeBool(self.blockCacheEnabled)
      oprot.writeFieldEnd()
    if self.timeToLive is not None:
      oprot.writeFieldBegin('timeToLive', TType.I32, 9)
      oprot.writeI32(self.timeToLive)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class TRegionInfo:
  """
  A TRegionInfo contains information about an HTable region.

  Attributes:
   - startKey
   - endKey
   - id
   - name
   - version
   - serverName
   - port
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'startKey', None, None, ), # 1
    (2, TType.STRING, 'endKey', None, None, ), # 2
    (3, TType.I64, 'id', None, None, ), # 3
    (4, TType.STRING, 'name', None, None, ), # 4
    (5, TType.BYTE, 'version', None, None, ), # 5
    (6, TType.STRING, 'serverName', None, None, ), # 6
    (7, TType.I32, 'port', None, None, ), # 7
  )

  def __init__(self, startKey=None, endKey=None, id=None, name=None, version=None, serverName=None, port=None,):
    self.startKey = startKey
    self.endKey = endKey
    self.id = id
    self.name = name
    self.version = version
    self.serverName = serverName
    self.port = port

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.startKey = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRING:
          self.endKey = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.I64:
          self.id = iprot.readI64();
        else:
          iprot.skip(ftype)
      elif fid == 4:
        if ftype == TType.STRING:
          self.name = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 5:
        if ftype == TType.BYTE:
          self.version = iprot.readByte();
        else:
          iprot.skip(ftype)
      elif fid == 6:
        if ftype == TType.STRING:
          self.serverName = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 7:
        if ftype == TType.I32:
          self.port = iprot.readI32();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('TRegionInfo')
    if self.startKey is not None:
      oprot.writeFieldBegin('startKey', TType.STRING, 1)
      oprot.writeString(self.startKey)
      oprot.writeFieldEnd()
    if self.endKey is not None:
      oprot.writeFieldBegin('endKey', TType.STRING, 2)
      oprot.writeString(self.endKey)
      oprot.writeFieldEnd()
    if self.id is not None:
      oprot.writeFieldBegin('id', TType.I64, 3)
      oprot.writeI64(self.id)
      oprot.writeFieldEnd()
    if self.name is not None:
      oprot.writeFieldBegin('name', TType.STRING, 4)
      oprot.writeString(self.name)
      oprot.writeFieldEnd()
    if self.version is not None:
      oprot.writeFieldBegin('version', TType.BYTE, 5)
      oprot.writeByte(self.version)
      oprot.writeFieldEnd()
    if self.serverName is not None:
      oprot.writeFieldBegin('serverName', TType.STRING, 6)
      oprot.writeString(self.serverName)
      oprot.writeFieldEnd()
    if self.port is not None:
      oprot.writeFieldBegin('port', TType.I32, 7)
      oprot.writeI32(self.port)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class Mutation:
  """
  A Mutation object is used to either update or delete a column-value.

  Attributes:
   - isDelete
   - column
   - value
   - writeToWAL
  """

  thrift_spec = (
    None, # 0
    (1, TType.BOOL, 'isDelete', None, False, ), # 1
    (2, TType.STRING, 'column', None, None, ), # 2
    (3, TType.STRING, 'value', None, None, ), # 3
    (4, TType.BOOL, 'writeToWAL', None, True, ), # 4
  )

  def __init__(self, isDelete=thrift_spec[1][4], column=None, value=None, writeToWAL=thrift_spec[4][4],):
    self.isDelete = isDelete
    self.column = column
    self.value = value
    self.writeToWAL = writeToWAL

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.BOOL:
          self.isDelete = iprot.readBool();
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRING:
          self.column = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.STRING:
          self.value = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 4:
        if ftype == TType.BOOL:
          self.writeToWAL = iprot.readBool();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('Mutation')
    if self.isDelete is not None:
      oprot.writeFieldBegin('isDelete', TType.BOOL, 1)
      oprot.writeBool(self.isDelete)
      oprot.writeFieldEnd()
    if self.column is not None:
      oprot.writeFieldBegin('column', TType.STRING, 2)
      oprot.writeString(self.column)
      oprot.writeFieldEnd()
    if self.value is not None:
      oprot.writeFieldBegin('value', TType.STRING, 3)
      oprot.writeString(self.value)
      oprot.writeFieldEnd()
    if self.writeToWAL is not None:
      oprot.writeFieldBegin('writeToWAL', TType.BOOL, 4)
      oprot.writeBool(self.writeToWAL)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class BatchMutation:
  """
  A BatchMutation object is used to apply a number of Mutations to a single row.

  Attributes:
   - row
   - mutations
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'row', None, None, ), # 1
    (2, TType.LIST, 'mutations', (TType.STRUCT,(Mutation, Mutation.thrift_spec)), None, ), # 2
  )

  def __init__(self, row=None, mutations=None,):
    self.row = row
    self.mutations = mutations

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.row = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.LIST:
          self.mutations = []
          (_etype3, _size0) = iprot.readListBegin()
          for _i4 in xrange(_size0):
            _elem5 = Mutation()
            _elem5.read(iprot)
            self.mutations.append(_elem5)
          iprot.readListEnd()
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('BatchMutation')
    if self.row is not None:
      oprot.writeFieldBegin('row', TType.STRING, 1)
      oprot.writeString(self.row)
      oprot.writeFieldEnd()
    if self.mutations is not None:
      oprot.writeFieldBegin('mutations', TType.LIST, 2)
      oprot.writeListBegin(TType.STRUCT, len(self.mutations))
      for iter6 in self.mutations:
        iter6.write(oprot)
      oprot.writeListEnd()
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class TIncrement:
  """
  For increments that are not incrementColumnValue
  equivalents.

  Attributes:
   - table
   - row
   - column
   - ammount
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'table', None, None, ), # 1
    (2, TType.STRING, 'row', None, None, ), # 2
    (3, TType.STRING, 'column', None, None, ), # 3
    (4, TType.I64, 'ammount', None, None, ), # 4
  )

  def __init__(self, table=None, row=None, column=None, ammount=None,):
    self.table = table
    self.row = row
    self.column = column
    self.ammount = ammount

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.table = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRING:
          self.row = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.STRING:
          self.column = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 4:
        if ftype == TType.I64:
          self.ammount = iprot.readI64();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('TIncrement')
    if self.table is not None:
      oprot.writeFieldBegin('table', TType.STRING, 1)
      oprot.writeString(self.table)
      oprot.writeFieldEnd()
    if self.row is not None:
      oprot.writeFieldBegin('row', TType.STRING, 2)
      oprot.writeString(self.row)
      oprot.writeFieldEnd()
    if self.column is not None:
      oprot.writeFieldBegin('column', TType.STRING, 3)
      oprot.writeString(self.column)
      oprot.writeFieldEnd()
    if self.ammount is not None:
      oprot.writeFieldBegin('ammount', TType.I64, 4)
      oprot.writeI64(self.ammount)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class TColumn:
  """
  Holds column name and the cell.

  Attributes:
   - columnName
   - cell
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'columnName', None, None, ), # 1
    (2, TType.STRUCT, 'cell', (TCell, TCell.thrift_spec), None, ), # 2
  )

  def __init__(self, columnName=None, cell=None,):
    self.columnName = columnName
    self.cell = cell

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.columnName = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRUCT:
          self.cell = TCell()
          self.cell.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('TColumn')
    if self.columnName is not None:
      oprot.writeFieldBegin('columnName', TType.STRING, 1)
      oprot.writeString(self.columnName)
      oprot.writeFieldEnd()
    if self.cell is not None:
      oprot.writeFieldBegin('cell', TType.STRUCT, 2)
      self.cell.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class TRowResult:
  """
  Holds row name and then a map of columns to cells.

  Attributes:
   - row
   - columns
   - sortedColumns
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'row', None, None, ), # 1
    (2, TType.MAP, 'columns', (TType.STRING,None,TType.STRUCT,(TCell, TCell.thrift_spec)), None, ), # 2
    (3, TType.LIST, 'sortedColumns', (TType.STRUCT,(TColumn, TColumn.thrift_spec)), None, ), # 3
  )

  def __init__(self, row=None, columns=None, sortedColumns=None,):
    self.row = row
    self.columns = columns
    self.sortedColumns = sortedColumns

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.row = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.MAP:
          self.columns = {}
          (_ktype8, _vtype9, _size7 ) = iprot.readMapBegin() 
          for _i11 in xrange(_size7):
            _key12 = iprot.readString();
            _val13 = TCell()
            _val13.read(iprot)
            self.columns[_key12] = _val13
          iprot.readMapEnd()
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.LIST:
          self.sortedColumns = []
          (_etype17, _size14) = iprot.readListBegin()
          for _i18 in xrange(_size14):
            _elem19 = TColumn()
            _elem19.read(iprot)
            self.sortedColumns.append(_elem19)
          iprot.readListEnd()
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('TRowResult')
    if self.row is not None:
      oprot.writeFieldBegin('row', TType.STRING, 1)
      oprot.writeString(self.row)
      oprot.writeFieldEnd()
    if self.columns is not None:
      oprot.writeFieldBegin('columns', TType.MAP, 2)
      oprot.writeMapBegin(TType.STRING, TType.STRUCT, len(self.columns))
      for kiter20,viter21 in self.columns.items():
        oprot.writeString(kiter20)
        viter21.write(oprot)
      oprot.writeMapEnd()
      oprot.writeFieldEnd()
    if self.sortedColumns is not None:
      oprot.writeFieldBegin('sortedColumns', TType.LIST, 3)
      oprot.writeListBegin(TType.STRUCT, len(self.sortedColumns))
      for iter22 in self.sortedColumns:
        iter22.write(oprot)
      oprot.writeListEnd()
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class TScan:
  """
  A Scan object is used to specify scanner parameters when opening a scanner.

  Attributes:
   - startRow
   - stopRow
   - timestamp
   - columns
   - caching
   - filterString
   - sortColumns
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'startRow', None, None, ), # 1
    (2, TType.STRING, 'stopRow', None, None, ), # 2
    (3, TType.I64, 'timestamp', None, None, ), # 3
    (4, TType.LIST, 'columns', (TType.STRING,None), None, ), # 4
    (5, TType.I32, 'caching', None, None, ), # 5
    (6, TType.STRING, 'filterString', None, None, ), # 6
    (7, TType.BOOL, 'sortColumns', None, None, ), # 7
  )

  def __init__(self, startRow=None, stopRow=None, timestamp=None, columns=None, caching=None, filterString=None, sortColumns=None,):
    self.startRow = startRow
    self.stopRow = stopRow
    self.timestamp = timestamp
    self.columns = columns
    self.caching = caching
    self.filterString = filterString
    self.sortColumns = sortColumns

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.startRow = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRING:
          self.stopRow = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.I64:
          self.timestamp = iprot.readI64();
        else:
          iprot.skip(ftype)
      elif fid == 4:
        if ftype == TType.LIST:
          self.columns = []
          (_etype26, _size23) = iprot.readListBegin()
          for _i27 in xrange(_size23):
            _elem28 = iprot.readString();
            self.columns.append(_elem28)
          iprot.readListEnd()
        else:
          iprot.skip(ftype)
      elif fid == 5:
        if ftype == TType.I32:
          self.caching = iprot.readI32();
        else:
          iprot.skip(ftype)
      elif fid == 6:
        if ftype == TType.STRING:
          self.filterString = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 7:
        if ftype == TType.BOOL:
          self.sortColumns = iprot.readBool();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('TScan')
    if self.startRow is not None:
      oprot.writeFieldBegin('startRow', TType.STRING, 1)
      oprot.writeString(self.startRow)
      oprot.writeFieldEnd()
    if self.stopRow is not None:
      oprot.writeFieldBegin('stopRow', TType.STRING, 2)
      oprot.writeString(self.stopRow)
      oprot.writeFieldEnd()
    if self.timestamp is not None:
      oprot.writeFieldBegin('timestamp', TType.I64, 3)
      oprot.writeI64(self.timestamp)
      oprot.writeFieldEnd()
    if self.columns is not None:
      oprot.writeFieldBegin('columns', TType.LIST, 4)
      oprot.writeListBegin(TType.STRING, len(self.columns))
      for iter29 in self.columns:
        oprot.writeString(iter29)
      oprot.writeListEnd()
      oprot.writeFieldEnd()
    if self.caching is not None:
      oprot.writeFieldBegin('caching', TType.I32, 5)
      oprot.writeI32(self.caching)
      oprot.writeFieldEnd()
    if self.filterString is not None:
      oprot.writeFieldBegin('filterString', TType.STRING, 6)
      oprot.writeString(self.filterString)
      oprot.writeFieldEnd()
    if self.sortColumns is not None:
      oprot.writeFieldBegin('sortColumns', TType.BOOL, 7)
      oprot.writeBool(self.sortColumns)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class IOError(TException):
  """
  An IOError exception signals that an error occurred communicating
  to the Hbase master or an Hbase region server.  Also used to return
  more general Hbase error conditions.

  Attributes:
   - message
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'message', None, None, ), # 1
  )

  def __init__(self, message=None,):
    self.message = message

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.message = iprot.readString();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('IOError')
    if self.message is not None:
      oprot.writeFieldBegin('message', TType.STRING, 1)
      oprot.writeString(self.message)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __str__(self):
    return repr(self)

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class IllegalArgument(TException):
  """
  An IllegalArgument exception indicates an illegal or invalid
  argument was passed into a procedure.

  Attributes:
   - message
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'message', None, None, ), # 1
  )

  def __init__(self, message=None,):
    self.message = message

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.message = iprot.readString();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('IllegalArgument')
    if self.message is not None:
      oprot.writeFieldBegin('message', TType.STRING, 1)
      oprot.writeString(self.message)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __str__(self):
    return repr(self)

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class AlreadyExists(TException):
  """
  An AlreadyExists exceptions signals that a table with the specified
  name already exists

  Attributes:
   - message
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'message', None, None, ), # 1
  )

  def __init__(self, message=None,):
    self.message = message

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.message = iprot.readString();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('AlreadyExists')
    if self.message is not None:
      oprot.writeFieldBegin('message', TType.STRING, 1)
      oprot.writeString(self.message)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __str__(self):
    return repr(self)

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)
