declareUpdate();

var URI;

var n = new NodeBuilder();
n.addElement("val", "Updated");
n = n.toNode(); 
xdmp.nodeReplace(cts.doc(URI).xpath("/root/val"), n);
