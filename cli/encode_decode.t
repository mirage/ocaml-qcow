Create a raw file with random contents
  $ dd if=/dev/random of=test.raw bs=65536 count=16 > /dev/null 2>&1

Convert it to qcow file and back to raw
  $ ./main.exe encode test.raw test.qcow2
  $ ./main.exe decode test.qcow2 transform.raw

Check that contents are the same
  $ diff test.raw transform.raw

Check stream_decode works the same as seeking decode
  $ cat test.qcow2 | ./main.exe stream_decode stream_transform.raw
  $ diff test.raw stream_transform.raw

Check we can decode files created by qemu-img
  $ qemu-img convert -f raw -O qcow2 test.raw qemu.qcow2
  $ ./main.exe decode qemu.qcow2 qemu_transform.raw
  $ diff test.raw qemu_transform.raw
  $ cat qemu.qcow2 | ./main.exe stream_decode stream_qemu_transform.raw
  $ diff test.raw stream_qemu_transform.raw

Check stream conversion of an empty raw file
  $ dd if=/dev/zero of=empty.raw bs=65536 count=16 > /dev/null 2>&1
  $ qemu-img convert -f raw -O qcow2 empty.raw empty.qcow2
  $ cat empty.qcow2 | ./main.exe stream_decode stream_emptytransform.raw
  $ diff empty.raw stream_emptytransform.raw
